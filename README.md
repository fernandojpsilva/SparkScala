# Spark Challenge
## Versions
Scala 2.11.8

jdk 1.8

## Possible errors (IntelliJ IDE)
> No run configuration

Make sure the src/main/scala folder is marked as "Sources Root" (blue folder in IntelliJ). If it is not, right click on the folder and select "Mark directory as > Sources Root"

>"Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/spark/sql/Dataset"

In Run/Debug configuration select "Modify Options" and check "Add dependencies with 'provided' scope to classpath"

