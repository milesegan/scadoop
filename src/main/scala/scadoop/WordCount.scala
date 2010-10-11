// scala / cascading

package scadoop

import cascading.flow.{Flow, FlowConnector}
import cascading.operation.{Aggregator, Function}
import cascading.operation.aggregator.Count
import cascading.operation.regex.RegexGenerator
import cascading.pipe.{Each, Every, GroupBy, Pipe}
import cascading.scheme.{Scheme, TextLine}
import cascading.tap.{Hfs, Tap}
import cascading.tap.SinkMode
import cascading.tuple.Fields
import java.util.Properties

object WordCount {

  def main(args:Array[String]) = {
    val inputPath = args(0)
    val outputPath = args(1)
    
    // define source and sink Taps.
    val sourceScheme = new TextLine(new Fields("line"))
    val source = new Hfs(sourceScheme, inputPath)

    val sinkScheme = new TextLine(new Fields("word", "count"))
    val sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE)

    // the 'head' of the pipe assembly
    var assembly = new Pipe("wordcount")

    // For each input Tuple
    // using a regular expression
    // parse out each word into a new Tuple with the field name "word"
    val regex = """([^ ]+)\s+"""
    val function = new RegexGenerator(new Fields("word"), regex)
    assembly = new Each(assembly, new Fields("line"), function)

    // group the Tuple stream by the "word" value
    assembly = new GroupBy(assembly, new Fields("word"))

    // For every Tuple group
    // count the number of occurrences of "word" and store result in
    // a field named "count"
    val count = new Count(new Fields("count"))
    assembly = new Every(assembly, count)

    // initialize app properties, tell Hadoop which jar file to use
    val properties = new Properties()
    FlowConnector.setApplicationJarClass(properties, WordCount.getClass)

    // plan a new Flow from the assembly using the source and sink Taps
    val flowConnector = new FlowConnector()
    val flow = flowConnector.connect("word-count", source, sink, assembly)

    // execute the flow, block until complete
    flow.complete()
  }
}
