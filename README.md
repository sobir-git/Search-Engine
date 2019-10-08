# A simple search engine application with Spark

## Source files
The directory structure is usual. Source files reside inside `src\main\scala` directory. They are:
1. **Indexer.scala** - Contains logic of Indexing.
4. **utils/Functions.scala** -  Contains utility functions like `vectorize_text()`, `normalize_word()`.
5. **Ranker** - Contains logic of Ranker, including the interactive querying.
6. **RelevanceAnalizator** - Contains ranker functions, namely simple inner product and BM25.

## How to compile
Simply run `sbt package` in the root directory of project. The resulting `jar` file will be `target/scala-2.11/searchengine_2.11-0.1.jar`.

## How to run
First the **Indexer** application should run to create index data and save it to a path. Then we can run **Ranker** on indexed data. 

### Indexer
Typically we run indexer in this format 
`spark-submit --master yarn --class Indexer <jar-file\> <input-path\> <output-path\>`

Example: 
`spark-submit --master yarn --class Indexer searchengine_2.11-0.1.jar /EnWikiMedium IndexDir`

Run with `-h` argument to see full help message

### Ranker
Typically we run ranker in this format 
`spark-submit --master yarn --class Ranker <jar-file\> -i <index-path\> <ranker-method> <search-query>`

Here `<ranker-method>` can be one of `inner` and `bm25`.

Example: 
`spark-submit --master yarn --class Ranker searchengine_2.11-0.1.jar -i IndexDir bm25 Game of Thrones`

Once you get the results of first query, the application will ask for the next query.

Run with `-h` argument to see full help message
