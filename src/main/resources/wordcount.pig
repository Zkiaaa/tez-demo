
lines =
    LOAD '$input'
    USING PigStorage() AS (line:chararray);

words =
    FOREACH lines
    GENERATE
        FLATTEN(TOKENIZE(line)) AS word;

grouped =
    GROUP words BY word;

counted =
    FOREACH grouped
    GENERATE
        group,
        COUNT(words);

STORE counted
    INTO '$output'
    USING PigStorage();
