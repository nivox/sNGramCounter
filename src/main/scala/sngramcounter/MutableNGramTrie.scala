package sngramcounter

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import scala.collection.JavaConversions._

import com.carrotsearch.hppc.IntObjectOpenHashMap
import com.carrotsearch.hppc.cursors.IntObjectCursor


class MutableNGramTrie(var count: Int, var subTrie: IntObjectOpenHashMap[MutableNGramTrie]) {

  def countNGram(ngram: Iterator[Int], n: Int = 1): Unit = {
    count += n

    if (ngram.hasNext()) {
      val unit = ngram.next
      var subNGramTrie: MutableNGramTrie = null
      if (subTrie.containsKey(unit)) {
        subNGramTrie = subTrie.get(unit)
      } else {
        subNGramTrie = MutableNGramTrie()
        subTrie.put(unit, subNGramTrie)
      }

      subNGramTrie.countNGram(ngram, n)
    }
  }

  private def iterateNGramsImpl(order: Int, acc: List[Int]): Iterator[(List[Int], Int)] = {
    if (order == 0) Iterator( (acc, count) )
    else if (subTrie.isEmpty()) Iterator.empty
    else {
      for (
        cursor <- subTrie.iterator;
        (ngram, v) <- cursor.value.iterateNGramsImpl(order-1, cursor.key::acc)
      ) yield (ngram, v)
    }
  }

  def iterateNGrams(order: Int): Iterator[(List[Int], Int)] = {
    iterateNGramsImpl(order, List()).map( {case (ngram, v) => (ngram.reverse, v)} )
  }

  def getCount: Int = count

  private def getUniqueCountImpl(order: Int): Iterator[Int] = {
    if (order == 1) Iterator(subTrie.size)
    else if (subTrie.isEmpty()) Iterator(0)
    else {
      for (
        cursor <- subTrie.iterator;
        n <- cursor.value.getUniqueCountImpl(order-1)
      ) yield n
    }
  }

  def getUniqueCount(order: Int): Int =
    getUniqueCountImpl(order).sum

  def dumpToFile(path: String, order: Int): Unit = {
    val fout = new BufferedOutputStream(new FileOutputStream(new File(path)))
    dumpToOutputStream(fout, order)
    fout.close()
  }

  def dumpToOutputStream(os: OutputStream, order: Int): Unit = {
    val buffer = ByteBuffer.allocate(4*(order + 1))

    iterateNGrams(order).foreach {
      case (ngram, count) =>
        buffer.clear()
        ngram.foreach( buffer.putInt(_) )
        buffer.putInt(count)

        os.write(buffer.array())
    }
  }
}


object MutableNGramTrie {
  def apply() = new MutableNGramTrie(0, new IntObjectOpenHashMap[MutableNGramTrie]())
}
