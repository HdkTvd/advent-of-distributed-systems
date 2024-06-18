# Solutions for advent-of-distributed-systems challenge

Follow installation guide at - [text](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md)

Steps to test this code using maelstrom -
1. run ```go install .``` in the project directory.
2. the executable file will be created at ~/go/bin
2. go to the dir where maelstrom binary is located.
3. run ```./maelstrom test -w echo --bin ~/go/bin/advent-of-distributed-systems.exe --time-limit 5```