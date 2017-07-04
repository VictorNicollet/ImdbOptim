using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Imdb
{
    /// <summary> Counts strings. </summary>
    /// <remarks>
    ///     Optimized to minimize allocations. Will only allocate one buffer for storing 
    ///     string contents, and containers-of-value-types for de-duplication and counting.
    /// </remarks>
    public sealed class StringCounter
    {
        /// <summary> A range of bytes within a buffer. </summary>
        /// <remarks>
        ///     This structure weighs 4 bytes in order to reduce the impact of 
        ///     having to load many of them in the processor cache. This is based 
        ///     on the assumption that we don't need to deal with pieces of text 
        ///     longer than 128 characters, and that we will have at most 2^25 bytes
        ///     (32 megabytes) of unique string data. 
        /// 
        ///     We could actually increase this to 128 megabytes of unique string data
        ///     by aligning the string data to four-byte boundaries. Beyond that, we 
        ///     should probably revert to an 8-byte range.
        /// </remarks>
        public struct Range
        {
            public Range(int start, int length)
            {
                if (length > 0x7F) length = 0x7F;
                Pack = ((uint)start << 7) | (uint)length;
            }

            public uint Pack { get; }

            public int Start => (int)(Pack >> 7);
            public int End => Start + Length;
            public int Length => (int)(Pack & 0x7F);
        }

        /// <summary> Contains the actual string data, and acts as a context for comparing ranges. </summary>
        /// <remarks>
        ///     An added benefit of providing our own <see cref="EqualityComparer{T}"/> instead of just adding
        ///     equals and hashcode to <see cref="Range"/> is that there's no boxing needed in the dictionary.
        /// </remarks>
        public sealed class StringData : IEqualityComparer<Range>
        {
            /// <summary> The buffer itself. </summary>
            /// <remarks> 
            ///     We pick the maximum possible size so we don't have to worry about resizing. 
            ///     32MB is small !
            /// 
            ///     The actual location of ranges in the buffer is managed in the <see cref="StringCounter"/>.
            /// </remarks>
            public readonly byte[] WordBuffer = new byte[32 * 1024 * 1024];           

            /// <see cref="IEqualityComparer{T}.Equals(T,T)"/>
            public bool Equals(Range x, Range y)
            {
                var len = x.Length;

                if (len != y.Length) return false;

                var sta = x.Start;
                var end = sta + len;

                var wb = WordBuffer;
                for (int i = sta, o = y.Start; i < end; ++i, ++o)
                    if (wb[i] != wb[o]) return false;

                return true;
            }

            /// <see cref="IEqualityComparer{T}.GetHashCode(T)"/>
            public int GetHashCode(Range r)
            {
                var wb = WordBuffer;
                var sta = r.Start;
                var end = sta + r.Length;
                var hash = 0;
                for (var i = sta; i < end; ++i) hash = (hash * 397) ^ wb[i];
                return hash;
            }
        }

        /// <summary> Binds a 'perfect hash' between 0 and count to each range. </summary>
        private readonly Dictionary<Range, int> _dict;

        /// <summary> The occurrence count of each range (indexed by perfect hash). </summary>
        private readonly List<int> _count = new List<int>();

        /// <summary> The buffer that backs the ranges in <see cref="_dict"/>. </summary>
        private readonly byte[] _buffer;

        /// <summary> The first byte in <see cref="_buffer"/> that isn't assigned to a real range. </summary>
        private int _end;

        public StringCounter()
        {
            var sd = new StringData();
            _dict = new Dictionary<Range, int>(sd);
            _buffer = sd.WordBuffer;

            // Load the stop words with a hash of '-1' so we don't count them.
            foreach (var stop in StopWords)
            {
                var length = Encoding.ASCII.GetBytes(stop, 0, stop.Length, _buffer, _end);
                _dict.Add(new Range(_end, length), -1);
                _end += length;
            }
        }

        /// <summary> Increment the count for a specific range. </summary>
        public void Count(byte[] data, int start, int length, int count = 1)
        {
            // Append the byte range to the word buffer (increasing its size if necessary)            
            Array.Copy(data, start, _buffer, _end, length);

            var range = new Range(_end, length);

            int pos;
            if (_dict.TryGetValue(range, out pos))
            {
                if (pos < 0) return;

                _count[pos] += count;
                return;
            }

            // Commit the word to the word buffer
            _end += length;

            // Generate a new identifier
            var id = _count.Count;
            _dict.Add(range, id);
            _count.Add(count);
        }

        /// <summary> Add the counts from another string counter. </summary>
        public void Add(StringCounter other)
        {
            foreach (var kv in other._dict)
                if (kv.Value >= 0)
                    Count(other._buffer, kv.Key.Start, kv.Key.Length, other._count[kv.Value]);
        }

        /// <summary> Extract the top words from this counter. </summary>
        public KeyValuePair<string, int>[] ExtractTop(int n)
        {
            // Always sorted by increasng number of occurrences. 
            var top = new KeyValuePair<Range, int>[n];
            var worst = 0;

            foreach (var kv in _dict)
            {
                if (kv.Value < 0) continue;
                var count = _count[kv.Value];

                if (count <= worst) continue;

                // Sorted insertion
                for (var i = 1; i <= n; ++i)
                {
                    if (i == n || count <= top[i].Value)
                    {
                        top[i - 1] = new KeyValuePair<Range, int>(kv.Key, count);
                        break;
                    }

                    top[i - 1] = top[i];
                }

                worst = top[0].Value;
            }

            return top
                .Select(t => new KeyValuePair<string, int>(
                    Encoding.UTF8.GetString(_buffer, t.Key.Start, t.Key.End - t.Key.Start),
                    t.Value))
                .Reverse()
                .ToArray();
        }

        /// <summary> All forbidden words. </summary>
        public static readonly string[] StopWords = { "a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "pl:", "mv:", "by:", "anonymous", "he's", "it's", "she's", "i'm", "#1" };
    }
}
