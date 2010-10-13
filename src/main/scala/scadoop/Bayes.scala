// scala / cascading

package scadoop

import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.operation.{Aggregator, AggregatorCall, BaseOperation, Debug, Function, FunctionCall, Identity, Insert}
import cascading.operation.aggregator.Count
import cascading.operation.regex.RegexSplitter
import cascading.pipe.{Each, Every, GroupBy, Pipe}
import cascading.scheme.{Scheme, TextDelimited, TextLine}
import cascading.tap.{Hfs, MultiSourceTap, Tap, SinkMode}
import cascading.tuple.{Fields, Tuple}

/**
 * Common argument parsing & feature manipulation logic.
 */
trait BayesOperation {
  val featureNames: Seq[String]
  
  def processArgs(line: String) = {
    val tokens = line.split(",").toList
    val klass = tokens.head
    val feats = (featureNames.tail zip tokens.tail).map{case(n,v) => n + "++" + v}
    (klass, feats)
  }
}

/**
 * Builds a classifier from supplied raw class, feature
 * tuples.
 */
class BayesBuilder(val featureNames: Seq[String])
extends BaseOperation[BayesClassifier](new Fields("bayes")) 
with Aggregator[BayesClassifier] with BayesOperation {

  def start(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    ac.setContext(BayesClassifier())
  }

  def aggregate(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    val bc = ac.getContext
    val (klass, feats) = processArgs(ac.getArguments.getString("line"))
    val newbc = bc.addSample(feats, klass)
    ac.setContext(newbc)
  }

  def complete(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    val bc = ac.getContext
    ac.getOutputCollector().add(new Tuple(bc.toJson))
  }

}

/**
 * Classifies incoming feature tuples with the supplied
 * classifier instance.
 */
class BayesProcessor(val featureNames: Seq[String], bc: BayesClassifier)
extends BaseOperation[BayesClassifier](new Fields("class", "predictedclass")) 
with Function[BayesClassifier] with BayesOperation {

  def operate(fp: FlowProcess, ac: FunctionCall[BayesClassifier]) {
    val (klass, feats) = processArgs(ac.getArguments.getString("line"))
    val predictedClass = bc.classify(feats).head._1
    ac.getOutputCollector().add(new Tuple(klass, predictedClass))
  }

}

object Bayes {

  /**
   * Reads the contents of the specified path via hfs.
   *
   * @return a sequence of lines from the file.
   */
  def readFile(path: String): Seq[String] = {
    import org.apache.hadoop.mapred.JobConf
    val tap = new Hfs(new TextLine(), path)
    val tuples = tap.openForRead(new JobConf())
    val b = collection.mutable.Buffer[String]()
    while (tuples.hasNext) {
      b += tuples.next.getString("line")
    }
    b.toSeq
  }

  def main(args:Array[String]) = {
    if (args.size < 1) throw new IllegalArgumentException()

    if (args(0) == "train") {
      assert(args.size == 4)
      val featNamePath = args(1)
      val inputPath = args(2)
      val outputPath = args(3)
      
      val featNames = readFile(featNamePath).head.split(",").toList
      var p = new Pipe("bayes")
      p = new Each(p, new Insert(new Fields("group"), "1"), Fields.ALL)
      p = new GroupBy(p, new Fields("group"))
      p = new Every(p, new BayesBuilder(featNames))
      p = new Each(p, new Fields("bayes"), new Identity)

      val source = new Hfs(new TextLine(), inputPath)
      val sink = new Hfs(new TextLine(), outputPath, SinkMode.REPLACE)
      val flowConnector = new FlowConnector()
      val flow = flowConnector.connect("bayes", source, sink, p)
      flow.complete()
    }
    else if (args(0) == "classify") {
      assert(args.size == 5)
      val featNames = readFile(args(1)).head.split(",").toList
      val bcJson = readFile(args(2)).head
      val bc = BayesClassifier(bcJson)
      val dataSource = new Hfs(new TextLine(), args(3))
      val sink = new Hfs(new TextLine(), args(4), SinkMode.REPLACE)
      var p = new Pipe("bayes")
      p = new Each(p, new BayesProcessor(featNames, bc))
      val flowConnector = new FlowConnector()
      val flow = flowConnector.connect("bayes", dataSource, sink, p)
      flow.complete()
    }
    else {
      throw new IllegalArgumentException()
    }
  }

}
