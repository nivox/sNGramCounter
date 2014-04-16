package sngramcounter

import scala.collection.mutable.{Map => MMap}

class Dictionary(private var total: Long, val wordsMap: MMap[String, (Int, Int)]) {

  def totalWords: Long = total

  def size: Int = wordsMap.size

  def getWordOccurrence(word: String): Option[Int] = wordsMap.get(word).map(_._2)

  def getWordIndex(word: String): Option[Int] = wordsMap.get(word).map(_._1)

  def splitIndexSetByOcc(n: Int): List[Set[Int]] = {
    val ordWords = wordsMap.toList.sortBy(_._2._2)

    def fillLast(lst: List[List[Int]], acc: List[List[Int]]): List[List[Int]] = lst match {
      case x::Nil => x.padTo(n, -1)::acc
      case x::xs => fillLast(xs, x::acc)
    }

    val filledWords = fillLast(ordWords.toList.map(_._2._1).grouped(n).toList, Nil)

    filledWords.transpose.map(_.toSet - -1)
  }

  def iterateWords: Iterator[(String, Int)] =
    wordsMap.iterator.map { case (w, (idx, occ)) => (w, occ) }

  def getIndexToWordMap: MMap[Int, String] =
    wordsMap.map { case (w, (idx,occ)) => (idx, w) }
}

object Dictionary {

  def empty = new Dictionary(0, MMap("<s>" -> (0,0)))

  def fromOccurrenceIterator(occIter: Iterator[(String, Int)]): Dictionary = {
    var total = 0
    val wordsMap: MMap[String, (Int, Int)] = MMap("<s>" -> (0,0))

    occIter.foreach { case (word, occ) =>
      total += occ
      wordsMap.get(word) match {
        case Some( (index, count) ) => wordsMap.update(word, (index, count+occ))
        case None => wordsMap.update(word, (wordsMap.size, occ) )
      }
    }

    new Dictionary(total, wordsMap)
  }

  def fromOccurrenceMap(occMap: Map[String, Int]): Dictionary =
    fromOccurrenceIterator(occMap.iterator)

  def fromWordsIterator(wordsIter: Iterator[String]): Dictionary =
    fromOccurrenceIterator(wordsIter.map( (_, 1) ))
}
