# Solutions for advent-of-distributed-systems challenge in GO using jepsen-io/maelstrom
<img src="https://go.dev/images/gophers/pilot-bust.svg" height="48" width="48"/>
<img src="https://avatars.githubusercontent.com/u/19334403?s=48&v=4" height="48" width="48"/>

Challenge link - [aods.cryingpotato.com](https://aods.cryingpotato.com/)

Follow instructions here - [Maelstrom Installation guide](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md)

Steps to test this code using maelstrom -
1. run ```go install .``` in the project directory.
2. the executable file will be created at ~/go/bin
2. go to the dir where maelstrom binary is located.
3. run ```./maelstrom test -w echo --bin ~/go/bin/advent-of-distributed-systems.exe --time-limit 5```