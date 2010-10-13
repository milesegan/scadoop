package scadoop

import com.twitter.json.{Json, JsonSerializable}
/**
 * Naive Bayesian classifier.
 */
class BayesClassifier private(
  val classes: Map[String,Double],
  val features: Map[String,Double],
  val featureClasses: Map[String,Map[String,Double]],
  val count: Double) extends JsonSerializable with java.io.Serializable {

  /**
   * Increments values in map for every key in keys.
   */
  private def incKeys[A](map: Map[A,Double], keys: Seq[A]) = {
    (map /: keys) { (m,k) => m.updated(k, m.getOrElse(k, 0d) + 1d) }
  }

  /**
   * Adds a sample to the map, returning a new map
   * incorporating it.
   */
  def addSample(feat: Seq[String], klass: String): BayesClassifier = {
    val newFs = incKeys(features, feat)
    val newCs = incKeys(classes, Seq(klass))
    val newFCs = feat.map { f => 
      f -> incKeys(featureClasses.getOrElse(f, Map()), Seq(klass))
    }
    new BayesClassifier(newCs, newFs, featureClasses ++ newFCs, count + 1d)
  }

  /**
   * Classify a new sample based on prior samples.
   *
   * @return A sequence of classes & their probabilities,
   * in order of decreasing likelihood.
   */
  def classify(feat: Seq[String]): Seq[(String,Double)] = {
    val ranked = for (c <- classes.keySet) yield {
      val probs = for (f <- feat) yield probability(f, c)
      (c, probs.product.toDouble * classes.getOrElse(c, 0d) / count)
    }
    ranked.toSeq.sortBy(_._2).reverse
  }

  /**
   * Computes the probability of class klass given
   * the feature.
   */
  def probability(f: String, klass: String): Double = {
    val pCF = featureClasses.getOrElse(f, Map()).getOrElse(klass, 0.05) // TODO: optimize fudge factor
    pCF / count
  }

  def toJson: String = {
    val m = Map("count" -> count,
                "classes" -> classes, 
                "features" -> features, 
                "feature-classes" -> featureClasses)
    Json.build(m).toString
  }

}

object BayesClassifier {
  def apply() = {
    new BayesClassifier(Map.empty, Map.empty, Map.empty, 0)
  }

  def apply(json: String) = {
    import util.parsing.json.JSON
    JSON.perThreadNumberParser = { s => s.toDouble }
    val m = JSON.parseFull(json).get.asInstanceOf[Map[String,_]]
    new BayesClassifier(m("classes").asInstanceOf[Map[String,Double]], 
                        m("features").asInstanceOf[Map[String,Double]], 
                        m("feature-classes").asInstanceOf[Map[String,Map[String,Double]]],
                        m("count").asInstanceOf[Double])
  }
}
