%{
#include <stdio.h>
#include <ctype.h>

int  regs[26];

%}

%start list

%token DIGIT LETTER

%left '+' '-'
%left '*' '/'

%%    /* beginning of rule section */

list : /* empty */
     | list stat '\n'
     ;

stat : expr
            { printf("%d\n", $1); }
     | LETTER '=' expr
            { regs[$1] = $3; }
     ;

expr : '(' expr ')'
            { $$ = $2; }
     | expr '+' expr
            { $$ = $1 + $3; }
     | expr '-' expr
            { $$ = $1 - $3; }
     | expr '*' expr
            { $$ = $1 * $3; }
     | expr '/' expr
            { $$ = $1 / $3; }
     | LETTER
            { $$ = regs[$1]; }
     | number
     ;

number : DIGIT
            { $$ = $1; }
       | number DIGIT
            { $$ = $1 * 10 + $2; }
       ;

%%    /* beginning of program */

yylex() {
    int c;
    while ((c = getchar()) == ' ') { /* skip blanks */ }

    if (islower(c)) {
        yylval = c - 'a';
        return LETTER;
    }
    if (isdigit(c)) {
        yylval = c - '0';
        return DIGIT;
    }
    return c;
}