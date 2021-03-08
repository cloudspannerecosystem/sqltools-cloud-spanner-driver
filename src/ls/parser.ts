export enum StatementType {
  UNSPECIFIED,
  QUERY,
  DML,
  DDL,
}

export class SpannerQueryParser {
  static getStatementType(sql: string): StatementType {
    if (!sql) {
      return StatementType.UNSPECIFIED;
    }
    const statement = SpannerQueryParser.getFirstKeywordOutsideComments(sql).toUpperCase();
    if (statement.startsWith('SELECT') || statement.startsWith('WITH')) {
      return StatementType.QUERY;
    }
    if (statement.startsWith('INSERT') || statement.startsWith('UPDATE') || statement.startsWith('DELETE')) {
      return StatementType.DML;
    }
    if (statement.startsWith('CREATE') || statement.startsWith('ALTER') || statement.startsWith('DROP')) {
      return StatementType.DDL;
    }
    return StatementType.UNSPECIFIED;
  }

  static getFirstKeywordOutsideComments(sql: string): string {
    var charArray: Array<string> = Array.from(sql);
    var previousChar: string = null;
    var nextChar: string = null;
    var isInComment: boolean = false;
    var commentChar: string = null;

    var keyword: string = '';
    for (var index = 0; index < charArray.length; index++) {
      var char = charArray[index];
      if (index > 0) {
        previousChar = charArray[index - 1];
      }

      if ((index + 1) < charArray.length) {
        nextChar = charArray[index + 1];
      } else {
        nextChar = null;
      }

      // it's comment, go to next char
      if (
        ((char == '#' && nextChar == ' ') || (char == '-' && nextChar == '-') || (char == '/' && nextChar == '*'))
      ) {
        isInComment = true;
        commentChar = char;
        continue;
      }
      // it's end of comment, go to next
      if (
        isInComment == true &&
        (((commentChar == '#' || commentChar == '-') && char == '\n') ||
          (commentChar == '/' && (char == '*' && nextChar == '/')))
      ) {
        if (commentChar == '/') {
          index++;
        }
        isInComment = false;
        commentChar = null;
        continue;
      }
      if (isInComment == false) {
        if (char.trim() == '') {
          if (keyword == '') {
            continue;
          }
          return keyword;
        }
        keyword = keyword + char;
      }
    }
    return keyword;
  }

  static parse(query: string): Array<string> {
    const delimiter: string = ';';
    var queries: Array<string> = [];
    var flag = true;
    while (flag) {
      if (restOfQuery == null) {
        restOfQuery = query;
      }
      var statementAndRest = SpannerQueryParser.getStatements(restOfQuery, delimiter);

      var statement = statementAndRest[0];
      if (statement != null && statement.trim() != '') {
        queries.push(statement);
      }

      var restOfQuery = statementAndRest[1];
      if (restOfQuery == null || restOfQuery.trim() == '') {
        break;
      }
    }

    return queries;
  }

  static getStatements(query: string, delimiter: string): Array<string> {
    var charArray: Array<string> = Array.from(query);
    var previousChar: string = null;
    var nextChar: string = null;
    var isInComment: boolean = false;
    var commentChar: string = null;
    var isInString: boolean = false;
    var stringChar: string = null;
    var isInTripleQuotes: boolean = false;

    var resultQueries: Array<string> = [];
    for (var index = 0; index < charArray.length; index++) {
      var char = charArray[index];
      if (index > 0) {
        previousChar = charArray[index - 1];
      }

      if ((index + 1) < charArray.length) {
        nextChar = charArray[index + 1];
      } else {
        nextChar = null;
      }

      // it's in string, go to next char
      if (previousChar != '\\' && (char == "'" || char == '"' || char == '`') && isInString == false && isInComment == false) {
        isInString = true;
        stringChar = char;
        if ((index + 2) < charArray.length) {
          if (charArray[index + 1] == char && charArray[index + 2] == char) {
            index += 2;
            isInTripleQuotes = true;
          }
        }
        continue;
      }

      // it's comment, go to next char
      if (
        ((char == '#' && nextChar == ' ') || (char == '-' && nextChar == '-') || (char == '/' && nextChar == '*')) &&
        isInString == false
      ) {
        isInComment = true;
        commentChar = char;
        continue;
      }
      // it's end of comment, go to next
      if (
        isInComment == true &&
        (((commentChar == '#' || commentChar == '-') && char == '\n') ||
          (commentChar == '/' && (char == '*' && nextChar == '/')))
      ) {
        isInComment = false;
        commentChar = null;
        continue;
      }

      // string closed, go to next char
      if (previousChar != '\\' && char == stringChar && isInString == true) {
        if (isInTripleQuotes) {
          if ((index + 2) < charArray.length && charArray[index + 1] == char && charArray[index + 2] == char) {
            index += 2;
            isInTripleQuotes = false;
          } else {
            continue;
          }
        }
        isInString = false;
        stringChar = null;
        continue;
      }

      // it's a query, continue until you get delimiter hit
      if (
        (char.toLowerCase() === delimiter.toLowerCase()) &&
        isInString == false &&
        isInComment == false
      ) {
        var splittingIndex = index + 1;
        resultQueries = SpannerQueryParser.getQueryParts(query, splittingIndex, 0);
        break;
      }
    }
    if (resultQueries.length == 0) {
      if (query != null) {
        query = query.trim();
      }
      resultQueries.push(query, null);
    }

    return resultQueries;
  }

  static getQueryParts(query: string, splittingIndex: number, numChars: number = 1): Array<string> {
    var statement: string = query.substring(0, splittingIndex);
    var restOfQuery: string = query.substring(splittingIndex + numChars);
    var result: Array<string> = [];
    if (statement != null) {
      statement = statement.trim();
    }
    result.push(statement);
    result.push(restOfQuery);
    return result;
  }

  static clearTextUntilComment(text: string): string {
    var nextChar: string = null;
    var charArray: Array<string> = Array.from(text);
    var clearedText: string = null;
    for (var index = 0; index < charArray.length; index++) {
      var char = charArray[index];

      if (index < charArray.length) {
        nextChar = charArray[index + 1];
      }

      if ((char == '#' && nextChar == ' ') || (char == '-' && nextChar == '-') || (char == '/' && nextChar == '*')) {
        break;
      } else {
        if (clearedText == null) {
          clearedText = '';
        }
        clearedText += char;
      }
    }

    return clearedText;
  }
}
