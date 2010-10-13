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

class BayesAggregator(featureNames: Seq[String])
extends BaseOperation[BayesClassifier](2, new Fields("bayes")) 
with Aggregator[BayesClassifier] {

  def start(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    ac.setContext(BayesClassifier())
  }

  def aggregate(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    val bc = ac.getContext
    val tokens = ac.getArguments.getString("line").split(",").toList
    val klass = tokens.head
    val feats = (featureNames zip tokens.tail).map{case(n,v) => n + "++" + v}
    val newbc = bc.addSample(feats, klass)
    ac.setContext(newbc)
  }

  def complete(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    val bc = ac.getContext
    ac.getOutputCollector().add(new Tuple(bc.toJson))
  }

}

object Bayes {

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

      val source = new Hfs(new TextLine(), inputPath)
      val sink = new Hfs(new TextLine(), outputPath, SinkMode.REPLACE)
      var p = new Pipe("bayes")
      p = new Each(p, new Insert(new Fields("group"), "1"), Fields.ALL)
      p = new GroupBy(p, new Fields("group"))
      p = new Every(p, new BayesAggregator(featNames))
      p = new Each(p, new Fields("bayes"), new Identity)
      val flowConnector = new FlowConnector()
      val flow = flowConnector.connect("bayes", source, sink, p)
      flow.complete()
    }
    else if (args(0) == "classify") {
      assert(args.size == 4)
      val bcSource = new Hfs(new TextLine(), args(1))
      val dataSource = new Hfs(new TextDelimited(new Fields("tokens"), false, ","),
                               args(2))
      val multi = new MultiSourceTap(bcSource, dataSource)
      val sink = new Hfs(new TextLine(), args(3), SinkMode.REPLACE)
      var p = new Pipe("bayes")
      p = new Each(p, new Debug())
      val flowConnector = new FlowConnector()
      val flow = flowConnector.connect("bayes", dataSource, sink, p)
      val jc = flow.getJobConf()
    }
    else {
      throw new IllegalArgumentException()
    }
  }

}
