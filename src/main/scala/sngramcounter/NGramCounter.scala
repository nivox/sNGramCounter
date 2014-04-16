package sngramcounter


import java.io.BufferedInputStream
import java.io.BufferedWriter
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.io.Source

object NGramCounter {

  val NODE_SIZE = 200
  val RATIO = 4

  val splitPtr = Pattern.compile("\\s+")

  def computePasses(totalWords: Long, dictSize: Int, order: Int): Int = {
    val dictRatio: Long = (totalWords/dictSize)*order
    val candidateMemory: Long = dictSize * NODE_SIZE * dictRatio * RATIO

    System.gc()

    val rt = Runtime.getRuntime()
    val availMemory: Long = rt.maxMemory - (rt.totalMemory - rt.freeMemory)

    Math.ceil(candidateMemory / availMemory.toDouble).toInt
  }

  def computeDictionary(inFile: File): Dictionary = {
    val lines = Source.fromFile(inFile).getLines()
    Dictionary.fromWordsIterator(lines.flatMap(splitPtr.split(_)))
  }

  def collectNGrams(inFile: File, binOutFile: File, dict: Dictionary, order: Int, passes: Int): (Long, Int) = {
    val idxSets = dict.splitIndexSetByOcc(passes)

    var totalNGrams: Long = 0
    var uniqueNGrams: Int = 0
    val fout = new FileOutputStream(binOutFile)

    idxSets.zipWithIndex.foreach {
      case (currIdxSet, pass) =>
        val lines = Source.fromFile(inFile).getLines()
        val ngramTrie: MutableNGramTrie = MutableNGramTrie()

        val ingram = ListBuffer.fill(order)(dict.getWordIndex("<s>").get)
        System.err.println("Counting ngrams (pass=" + pass + " subdict=" + currIdxSet.size + ")")
        var lineNr = 0
        lines.foreach { l =>
          lineNr += 1
          if (lineNr % 1000 == 0) System.err.print("\rLine nr: " + lineNr);
          splitPtr.split(l).foreach{ w =>
            val u = dict.getWordIndex(w)
            ingram.remove(0)
            ingram.append(u.get)
            if (currIdxSet.contains(u.get)) ngramTrie.countNGram(ingram.iterator)
          }
        }

        val currTotalNGrams = ngramTrie.getCount
        val currUniqueNGrams = ngramTrie.getUniqueCount(order)

        totalNGrams += currTotalNGrams
        uniqueNGrams += currUniqueNGrams

        System.err.println("\rTotal ngrams for pass " + pass + ": " + currTotalNGrams)
        System.err.println("\rUnique ngrams for pass " + pass + ": " + currUniqueNGrams)
        ngramTrie.dumpToOutputStream(fout, order)
    }
    fout.close()
    return (totalNGrams, uniqueNGrams)
  }

  def writeNGramFile(outFile: File, dumpFile: File, dict: Dictionary, order: Int, total: Long, uniques: Int): Unit = {
    val inS = new BufferedInputStream(new FileInputStream(dumpFile))
    val outW = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)))

    outW.write(s"nGrAm ${order} ${uniques} ngram\n")
    outW.write(s"${dict.size}\n")
    dict.iterateWords.foreach { case (w, occ) => outW.write(s"${w} ${occ}\n") }
    val keyWordMap = dict.getIndexToWordMap

    val buffer = ByteBuffer.allocate(4*(order + 1))
    val rawBuffer = new Array[Byte](4*(order + 1))
    (0 until uniques).foreach { i =>
      val r = inS.read(rawBuffer)
      if (r < rawBuffer.length) throw new IllegalStateException("Not all bytes read")

      buffer.clear
      buffer.put(rawBuffer)
      buffer.flip

      val idxLst = ListBuffer[Int]()
      (0 until order).foreach( n => idxLst.append(buffer.getInt()) )

      val count = buffer.getInt
      val wordLst = idxLst.map( keyWordMap(_) )

      outW.write(s"${wordLst mkString ""} ${count}\n")
    }
  }

  def main(args: Array[String]) = {
    val inFile = new File(args(0))
    val outFile = new File(args(1))
    val n = args(2).toInt

    System.err.println("Computing dictionary...")
    val dict = computeDictionary(inFile)
    val passes = computePasses(dict.totalWords, dict.size, n)

    println("Dictionary size: " + dict.size)
    println("Total words: " + dict.totalWords)
    println("Passes: " + passes)

    val dumpFile = File.createTempFile("sNGramCounter", "bin")
    System.out.println(s"Using dump file: ${dumpFile.getCanonicalPath}")
    val (total, uniques) = collectNGrams(inFile, dumpFile, dict, n, passes)

    writeNGramFile(outFile, dumpFile, dict, n, total, uniques)

    // val splitPtr = Pattern.compile("\\s+")
    // val lines = Source.fromFile(inFile).getLines()
    // val dictMap: ObjectIntOpenHashMap[String] = new ObjectIntOpenHashMap[String]()
    // val ngramTrie: MutableNGramTrie = MutableNGramTrie()

    // dictMap.put("<s>", 0)
    // val ingram = ListBuffer.fill(n)(0)

    // System.err.println("Counting ngrams")
    // var lineNr = 0
    // lines.foreach { l =>
    //   lineNr += 1
    //   if (lineNr % 1000 == 0) System.err.print("\rLine nr: " + lineNr);
    //   splitPtr.split(l).foreach{ w =>
    //     val u = indexWord(dictMap, w)
    //     ingram.remove(0)
    //     ingram.append(u)
    //     ngramTrie.countNGram(ingram.iterator)
    //   }
    // }

    // System.err.println("Saving ngrams")
    // ngramTrie.iterateNGrams(4).foreach(println)
  }
}
