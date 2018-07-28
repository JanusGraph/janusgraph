#region License

/*
 * Copyright 2018 JanusGraph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#endregion

using Gremlin.Net.Process.Traversal;

namespace JanusGraph.Net.Extensions
{
    /// <summary>
    ///     Provides text search predicates.
    /// </summary>
    public static class Text
    {
        /// <summary>
        ///     Is true if (at least) one word inside the text string matches the query string.
        /// </summary>
        /// <param name="query">The query to search.</param>
        /// <returns>The text predicate.</returns>
        public static P TextContains(string query)
        {
            return new P("textContains", query);
        }
        
        
        /// <summary>
        ///     Is true if (at least) one word inside the text string begins with the query string.
        /// </summary>
        /// <param name="query">The query to search.</param>
        /// <returns>The text predicate.</returns>
        public static P TextContainsPrefix(string query)
        {
            return new P("textContainsPrefix", query);
        }
        
        /// <summary>
        ///     Is true if (at least) one word inside the text string matches the given regular expression.
        /// </summary>
        /// <param name="regex">The regular expression.</param>
        /// <returns>The text predicate.</returns>
        public static P TextContainsRegex(string regex)
        {
            return new P("textContainsRegex", regex);
        }

        /// <summary>
        ///     Is true if (at least) one word inside the text string is similar to the query String (based on Levenshtein edit
        ///     distance).
        /// </summary>
        /// <param name="query">The query to search.</param>
        /// <returns>The text predicate.</returns>
        public static P TextContainsFuzzy(string query)
        {
            return new P("textContainsFuzzy", query);
        }
        
        /// <summary>
        ///     Is true if the string value starts with the given query string.
        /// </summary>
        /// <param name="query">The query to search.</param>
        /// <returns>The text predicate.</returns>
        public static P TextPrefix(string query)
        {
            return new P("textPrefix", query);
        }
        
        /// <summary>
        ///     Is true if the string value matches the given regular expression in its entirety.
        /// </summary>
        /// <param name="regex">The regular expression.</param>
        /// <returns>The text predicate.</returns>
        public static P TextRegex(string regex)
        {
            return new P("textRegex", regex);
        }

        /// <summary>
        ///     Is true if the string value is similar to the given query string (based on Levenshtein edit distance).
        /// </summary>
        /// <param name="query">The query to search.</param>
        /// <returns>The text predicate.</returns>
        public static P TextFuzzy(string query)
        {
            return new P("textFuzzy", query);
        }
    }
}
