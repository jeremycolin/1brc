# 1brc

Having fun with the one billion row challenge

# Setup

Clone [gunnarmorling/1brc](https://github.com/gunnarmorling/1brc) and [prepare](https://github.com/gunnarmorling/1brc your database

# The way it works

main splits the file into different worker threads at the right place (a newline) and workes divide and conquer

# Approaches

## With transform-stream-worker.ts as worker use custom Transform stream

## With worker.ts use a manual stream
