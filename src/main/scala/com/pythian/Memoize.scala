package com.pythian

/* Simple (not a lock-free!) thread-safe memoization function with keeping last `size` results */
class Memoize[K, V](size: Int)(fun: K ⇒ V) {
  private val cache = scala.collection.concurrent.TrieMap.empty[K, V]
  private val queue = scala.collection.mutable.Queue.empty[K]
  def apply(k: K): V =
    cache.getOrElse(k,
      this.synchronized {
        cache.getOrElse(k, {
          if (queue.size >= size) {
            cache.remove(queue.dequeue())
          }
          queue.enqueue(k)
          val v = fun(k)
          cache.put(k, v)
          v
        })
    })
}
