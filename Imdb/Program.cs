using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Imdb
{
    public sealed class Program
    {
        
        /// <summary> Reads from the input stream into the provided buffer. </summary>
        /// <remarks>
        ///     Reads starting at position <paramref name="offset"/> into the buffer. 
        ///     We can use this to store the bytes not used by the previous pass into
        ///     the buffer. Returns the number of bytes read.
        /// </remarks>
        private static int ReadAsync(Stream s, byte[] buf, int offset)
        {
            var startOffset = offset;
            while (offset < buf.Length)
            {
                //var read = await s.ReadAsync(buf, offset, buf.Length - offset).ConfigureAwait(false);
                var read = s.Read(buf, offset, buf.Length - offset);
                if (read == 0) break;

                offset += read;
            }

            return offset - startOffset;
        }

        /// <summary> The size of the buffers used for reading. </summary>
        /// <remarks>
        ///     The larger they are, the lower the overhead of context-swapping between reading 
        ///     data and processing data. Mostly has an effect on the multi-thread version.
        /// </remarks>
        private const int BufferSize = 40 * 1024 * 1024;

        /// <summary>
        /// Return the position of the last splitting character in <paramref name="buffer"/>,
        /// moving backwards from <paramref name="length"/>. If no splitting character appears,
        /// returns <paramref name="length"/>.
        /// </summary>
        private static int LastSplit(byte[] buffer, int length)
        {
            var split = length - 1;
            for (; split >= 0; --split)
            {
                var c = buffer[split];
                if (c == ' ' || c == ',' || c == '.' || c == '\n' || c == '\r') return split;
            }

            return length;
        }

        /// <summary> 
        ///     Look for the next valid word in the buffer. Alters the buffer contents so that the
        ///     word will be in lowercase.
        /// </summary>        
        private static bool NextWord(byte[] buffer, int length, ref int start, out int end)
        {
            var discard = false;

            for (end = start; end < length; ++end)
            {
                var c = buffer[end];

                if (c >= 'a' && c <= 'z') continue; // This will be the most frequent case

                if (c >= 'A' && c <= 'Z')
                {
                    // Less frequent, and we want to convert to lowercase to simplify comparisons later on
                    buffer[end] = (byte)(buffer[end] - 'A' + 'a');
                    continue;
                }

                if (c == ' ' || c == ',' || c == '.' || c == '\n' || c == '\r')
                {
                    // Split character found. If we have at least one character, then we found a word.
                    if (start < end && !discard) return true;

                    // Otherwise, start looking from the next position.
                    discard = false;
                    start = end + 1;
                    continue;
                }

                if (start < end) continue; // Invalid characters are only detected at the beginning of a word

                if (c == '"' || c == '\'' || c == '!' || c == '(' ||
                    c == ')' || c == '{' || c == '}' || c == '<' ||
                    c == '>' || c == '|' || c == '?' || c == '-' ||
                    c == '_' || c == '&')
                {
                    discard = true;
                }
            }

            return start < end;
        }

        /// <summary> Process all words found in the buffer. </summary>
        private static void Process(StringCounter sc, byte[] buffer, int length)
        {
            var start = 0;
            int end;

            while (NextWord(buffer, length, ref start, out end))
            {
                //Console.WriteLine(Encoding.UTF8.GetString(buffer, start, end - start));
                sc.Count(buffer, start, end - start);
                start = end;
            }
        }

        static void Main()
        {
            //OptimizedSingleThread();
            OptimizedMultiThread(4).Wait();
            //GetTopWordsSequential();
        }

        static void OptimizedSingleThread()
        {
            var sw = Stopwatch.StartNew();

            var buffer = new byte[BufferSize];
            var sc = new StringCounter();

            using (var gz = File.OpenRead(@"C:\Users\victo\Downloads\plot.list"))
            {
                var offset = 0;
                while (true)
                {
                    var read = ReadAsync(gz, buffer, offset);
                    var length = offset + read;
                    var split = LastSplit(buffer, length);
                    
                    Process(sc, buffer, split);
                    
                    if (split < length) Array.Copy(buffer, split, buffer, 0, length - split);

                    Console.WriteLine($"{sw.Elapsed} - {100 * gz.Position / gz.Length:D3}%");

                    if (read == 0) break;
                }
            }

            var top = sc.ExtractTop(TopCount);

            Console.WriteLine($"{sw.Elapsed} top words extracted");

            foreach (var t in top)
                Console.WriteLine("{0}: {1}", t.Key, t.Value);
        }

        static async Task OptimizedMultiThread(int threads)
        {
            var sw = Stopwatch.StartNew();
            
            var buffers = Enumerable.Range(0, threads).Select(t => new byte[BufferSize]).ToArray();
            var counts = Enumerable.Range(0, threads).Select(t => new StringCounter()).ToArray();
            var tasks = Enumerable.Range(0, threads).Select(t => (Task)Task.FromResult(0)).ToArray();
            
            using (var stream = File.OpenRead(@"C:\Users\victo\Downloads\plot.list"))
            {
                var split = 0;
                var length = 0;
                byte[] lastBuffer = null;

                while (true)
                {
                    // Look for a completed task.
                    var i = 0;
                    while (i < tasks.Length && !tasks[i].IsCompleted) ++i;

                    // No completed task: wait for one, then look again.
                    if (i == tasks.Length)
                    {
                        await Task.WhenAny(tasks).ConfigureAwait(false);
                        i = 0;
                        while (i < tasks.Length && !tasks[i].IsCompleted) ++i;
                    }

                    var myBuffer = buffers[i];
                    var myCount = counts[i];

                    // If there was some data left at the end of the last buffer we read, copy it.
                    var offset = 0;
                    if (lastBuffer != null)
                        Array.Copy(lastBuffer, split, myBuffer, 0, offset = length - split);

                    // Read new data into the buffer for this task.
                    var read = ReadAsync(stream, myBuffer, offset);
                    length = offset + read;

                    // Do we need to spill anything into the next buffer ? Don't split if at end-of-stream.
                    var myLength = split = read > 0 ? LastSplit(myBuffer, length) : length;
                    lastBuffer = split < length ? myBuffer : null;
                    
                    tasks[i] = Task.Run(() => Process(myCount, myBuffer, myLength));
                    
                    Console.WriteLine($"{sw.Elapsed} - {100 * stream.Position / stream.Length:D3}%");

                    if (read == 0) break; 
                }

                // Wait for everything to complete
                await Task.WhenAll(tasks);
                
                Console.WriteLine($"{sw.Elapsed} - Counting done.");

                for (var i = 1; i < threads; ++i)
                    counts[0].Add(counts[i]);
                
                Console.WriteLine($"{sw.Elapsed} - Reduce done.");
            }

            var top = counts[0].ExtractTop(TopCount);

            Console.WriteLine($"{sw.Elapsed} top words extracted");

            foreach (var t in top)
                Console.WriteLine("{0}: {1}", t.Key, t.Value);
        }

        private const int TopCount = 100;

        #region Comparison

        private static readonly HashSet<string> TheStopWords = new HashSet<string>(StringCounter.StopWords, StringComparer.OrdinalIgnoreCase);

        private static void GetTopWordsSequential()
        {
            var sw = Stopwatch.StartNew();

            Console.WriteLine(nameof(GetTopWordsSequential) + "...");

            var result = new Dictionary<string, uint>(StringComparer.InvariantCultureIgnoreCase);
            foreach (var line in File.ReadLines(@"C:\Users\victo\Downloads\plot.list"))
            {
                foreach (var word in line.Split(Separators, StringSplitOptions.RemoveEmptyEntries))
                {
                    if (!IsValidWord(word)) { continue; }
                    TrackWordsOccurrence(result, word);
                }
            }
            
            foreach (var t in result
                .OrderByDescending(kv => kv.Value)
                .Take((int) TopCount))
            {
                Console.WriteLine("{0}: {1}", t.Key, t.Value);
            }

            Console.WriteLine("{0} done", sw.Elapsed);
        }

        private static readonly char[] Separators = { ' ', '.', ',' };
        private static readonly HashSet<char> InvalidCharacters = new HashSet<char>(new[] { '®', '"', '\'', '!', '(', ')', '{', '}', '<', '>', '|', '?', '-', '_', '&' });
        
        private static void TrackWordsOccurrence(IDictionary<string, uint> wordCounts, string word)
        {
            if (wordCounts.TryGetValue(word, out uint count))
            {
                wordCounts[word] = count + 1;
            }
            else
            {
                wordCounts[word] = 1;
            }
        }

        private static bool IsValidWord(string word) => !InvalidCharacters.Contains(word[0]) && !TheStopWords.Contains(word);

        #endregion
    }
}
