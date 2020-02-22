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
            for (int i = leftParenthesisIndex + 1; i < chars.Length; ++i)
            {
                if (chars[i] == '(')
                {
                    lefts++;
                }
                else if (chars[i] == ')')
                {
                    if (++rights == lefts)
                    {
                        return text.Substring(leftParenthesisIndex + 1, i - (leftParenthesisIndex + 1));
                    }
                }
            }
            throw new Exception("Text does not have matching parentheses.");
        }

        public static string Lengthen(this string text, int characters)
        {
            int length = 0;
            var builder = new StringBuilder();
            while (length < characters)
            {
                builder.Append(text);
                length += text.Length;
            }
            return builder.ToString().Substring(0, characters);
        }        
    }
}
