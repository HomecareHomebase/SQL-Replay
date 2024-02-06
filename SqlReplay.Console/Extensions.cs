namespace SqlReplay.Console
{
    using System;
    using System.Text;

    public static class Extensions
    {
        public static string GetParenthesesContent(this string text)
        {
            return GetParenthesesContent(text, text.IndexOf('('));
        }

        public static string GetParenthesesContent(this string text, int leftParenthesisIndex)
        {
            char[] chars = text.ToCharArray();
            int lefts = 1;
            int rights = 0;
            bool withinQuotes = false;
            for (int i = leftParenthesisIndex + 1; i < chars.Length; ++i)
            {
                if (chars[i] == '\'')
                {
                    withinQuotes = !withinQuotes;
                }
                else if (chars[i] == '(' && !withinQuotes)
                {
                    lefts++;
                }
                else if (chars[i] == ')' && !withinQuotes)
                {
                    if (++rights == lefts)
                    {
                        return text.Substring(leftParenthesisIndex + 1, i - (leftParenthesisIndex + 1));
                    }
                }
            }
            throw new Exception("Text does not have matching parentheses.");
        }   
    }
}
