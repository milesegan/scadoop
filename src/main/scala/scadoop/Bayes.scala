// scala / cascading

package scadoop

import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.operation.{Aggregator, AggregatorCall, BaseOperation, Debug, Function, FunctionCall, Identity}
import cascading.operation.aggregator.Count
import cascading.operation.regex.RegexSplitter
import cascading.pipe.{Each, Every, GroupBy, Pipe}
import cascading.scheme.{Scheme, TextDelimited, TextLine}
import cascading.tap.{Hfs, Tap}
import cascading.tap.SinkMode
import cascading.tuple.{Fields, Tuple}
import collection.mutable.{Map => MMap}

class BayesSplitter 
extends BaseOperation[Array[String]](new Fields("dummy", "class", "features")) 
with Function[Array[String]] {

  def operate(fp: FlowProcess, fc: FunctionCall[Array[String]]) = {
    val tokens = fc.getArguments.getString("tokens")
    val str = tokens.toString.split(",")
    var names = fc.getContext
    if (names == null) {
      fc.setContext(str.tail)
    }
    else {
      val feats = new Tuple()
      for (f <- names zip str.tail) feats.add(f._1 + "++" + f._2)
      fc.getOutputCollector().add(new Tuple("-", str.head, feats))
    }
  }

}

class BayesAggregator
extends BaseOperation[BayesClassifier](2, new Fields("bayes")) 
with Aggregator[BayesClassifier] {

  def start(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    ac.setContext(BayesClassifier())
  }

  def aggregate(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    val bc = ac.getContext
    val klass = ac.getArguments.getString("class")
    val featT = ac.getArguments.getObject("features").asInstanceOf[Tuple]
    val feats = (0 until featT.size).map(featT.getString(_))
    val newbc = bc.addSample(feats, klass)
    ac.setContext(newbc)
  }

  def complete(fp: FlowProcess, ac: AggregatorCall[BayesClassifier]) {
    val bc = ac.getContext
    ac.getOutputCollector().add(new Tuple(bc))
  }

}

object Bayes {

  def main(args:Array[String]) = {
    val inputPath = args(0)
    val outputPath = args(1)
    
    val sourceScheme = new TextDelimited(new Fields("tokens"), false, ",")
    val source = new Hfs(sourceScheme, inputPath)
    val sinkScheme = new TextLine(new Fields("bc"))
    val sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE)

    var p = new Pipe("bayes")
    p = new Each(p, new BayesSplitter)
    p = new GroupBy(p, new Fields("dummy"))
    p = new Every(p, new BayesAggregator)
    p = new Each(p, new Fields("bayes"), new Identity)

    val flowConnector = new FlowConnector()
    val flow = flowConnector.connect("bayes", source, sink, p)

    flow.complete()
  }

}
