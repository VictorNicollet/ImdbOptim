# Optimized C# word counter

Based on [this article about parallelizing a word counter](http://www.nimaara.com/2017/07/01/practical-parallelization-with-map-reduce-in-c/),
from which the original `GetTopWordsSequential` is taken.

The point of that article was to illustrate various ways to parallelize data processing in C#.
The point of this version (which is inspired by that article, rather than a response to it) is
that while maximizing wall clock speed is one optimization target (you get results faster), 
maximizing CPU speed is usually a better one (you get results faster _for the same price_), and 
once a naive algorithm is determined to be critical, it can usually be improved by an order of 
magnitude with a few simple tricks, simply by knowing the cost of the various abstractions in
.NET and C#.

Benchmarks on my machine: 

 - GetTopWordsSequential:            **92 seconds** (not including StopWord initialization)
 - OptimizedSingleThread:            **22 seconds**
 - OptimizedMultiThread(threads: 4)  **10 seconds**

See `StringCounter.cs` for details of this optimization. The general idea is that, instead of
Sallocating `string` values, we work with a big `byte[]` buffer, so we save time on: 

  - not having to parse the on-disk bytes into an encoding
  - not having to allocate many `string` and `string[]` all over the place (and using less
    memory in general, which leads to less cache misses)
  - not having to frequently GC through millions of strings (our GCs are both smaller, because
    we only have a handful of objects, all of them on the LoH, and less frequent, because 
	there's less allocation pressure).

The multi-threaded version uses a "free task pool" pattern during the map phase to allocate 
work to tasks based on which tasks are available. It does not attempt to parallelize the 
reduce phase, which takes less than half a second.

