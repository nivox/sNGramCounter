package sngramcounter

import scala.collection.mutable.Map

/**
 * ADT representing an NGramTrie associating values of type V to ngrams formed
 * by units of type U
 */
sealed trait NGramTrie[+U, +V]
case object EmptyNGramTrie extends NGramTrie[Nothing, Nothing]
case class NGramNode[U, @specialized(Int, Long) V](data: V, subTrie: Map[U, NGramTrie[U, V]]) extends NGramTrie[U, V]
case class NGramLeaf[U, @specialized(Int, Long) V](data: V) extends NGramTrie[U, V]

/**
 * Functions for operating with NGramTrie
 */
object NGramTrie {

  def apply(): NGramTrie[Nothing, Nothing] = EmptyNGramTrie
  /**
   * Returns an empty NGramTrie
   */
  def emptyNGramTrie(): NGramTrie[Nothing, Nothing] = EmptyNGramTrie

  def getTopValue[U, V](ngramTrie: NGramTrie[U, V]): Option[V] = {
    ngramTrie match {
      case EmptyNGramTrie => None
      case NGramNode(v, _) => Some(v)
      case NGramLeaf(v) => Some(v)
    }
  }

  /**
   * Inserts the ngram in the specified ngramTrie using the provided function f
   * to initialize/update the value associated to each ngram.
   */
  def insertNGram[U, V](ngram: List[U], ngramTrie: NGramTrie[U, V], f: (Option[V] => V)): NGramTrie[U, V] =
    ngram match {
      case Nil =>
        ngramTrie match {
          case EmptyNGramTrie => new NGramLeaf(f(None))
          case NGramNode(v, subTrie: Map[U, NGramTrie[U, V]]) => new NGramNode(f(Some(v)), subTrie)
          case NGramLeaf(v) => new NGramLeaf(f(Some(v)))
        }

      case u::subgram =>
        ngramTrie match {
          case EmptyNGramTrie => new NGramNode(f(None), Map(u -> insertNGram(subgram, EmptyNGramTrie, f)))
          case NGramNode(v, subTrie: Map[U, NGramTrie[U, V]]) => {
            //new NGramNode(f(Some(v)), subTrie.updated(u, insertNGram(subgram, subTrie.getOrElse(u, EmptyNGramTrie), f)))
            subTrie.update(u, insertNGram(subgram, subTrie.getOrElse(u, EmptyNGramTrie), f))
            new NGramNode(f(Some(v)), subTrie)
          }
          case NGramLeaf(v) => new NGramNode(f(Some(v)), Map(u -> insertNGram(subgram, EmptyNGramTrie, f)))
        }
    }

  /**
   * Produces a stream of all ngrams of the specified order alongside their associated data
   */
  def streamNGrams[U, V](ngramTrie: NGramTrie[U, V], order: Int): Stream[(List[U], V)] = {
    def streamNGrams1[U, V](ngramTrie: NGramTrie[U, V], order: Int, acc: List[U]): Stream[(List[U], V)] =
      ngramTrie match {
        case EmptyNGramTrie => Stream[(List[U], V)]()
        case NGramLeaf(v) if order > 0  => Stream[(List[U], V)]()
        case NGramLeaf(v) if order == 0 => Stream[(List[U], V)]( (acc, v) )
        case NGramNode(v, subTrie) if order > 0 =>
          for (
            (u, nSubTrie) <- subTrie.toStream;
            (ngram, v) <- streamNGrams1(nSubTrie, order-1, u::acc)
          ) yield (ngram, v)
        case NGramNode(v, subTrie) if order == 0 =>
          Stream[(List[U], V)]( (acc, v) )
      }

    streamNGrams1(ngramTrie, order, List()).map( {case (ngram, v) => (ngram.reverse, v)} )
  }


  def iterateNGrams[U, V](ngramTrie: NGramTrie[U, V], order: Int): Iterator[(List[U], V)] = {
    def iterateNGrams1[U, V](ngramTrie: NGramTrie[U, V], order: Int, acc: List[U]): Iterator[(List[U], V)] =
      ngramTrie match {
        case EmptyNGramTrie => Iterator.empty
        case NGramLeaf(v) if order > 0  => Iterator.empty
        case NGramLeaf(v) if order == 0 => List( (acc, v) ).iterator
        case NGramNode(v, subTrie) if order > 0 =>
          for (
            (u, nSubTrie) <- subTrie.toIterator;
            (ngram, v) <- iterateNGrams1(nSubTrie, order-1, u::acc)
          ) yield (ngram, v)
        case NGramNode(v, subTrie) if order == 0 =>
          List( (acc, v) ).iterator
      }

    iterateNGrams1(ngramTrie, order, List()).map( {case (ngram, v) => (ngram.reverse, v)} )
  }
}
