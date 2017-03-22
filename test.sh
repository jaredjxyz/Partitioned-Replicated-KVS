# build.sh kills all docker containers, rebuilds container when you edit source
# running containers
docker kill $(docker ps -q)
docker build -t hw4 .

python tests.py
# tmux new-session -d -s pitted
# tmux split-window -t pitted 'docker run -p 8081:8080 --net=mynet --ip=10.0.0.21 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080" -e "IPPORT"="10.0.0.21:8080" hw4'
# tmux select-layout -t pitted main-horizontal
# tmux split-window -h -p 50 -t pitted  'docker run -p 8082:8080 --net=mynet --ip=10.0.0.22 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080" -e "IPPORT"="10.0.0.22:8080" hw4'
# # tmux select-pane -U -t pitted
# # tmux select-layout -t pitted tiled
# # tmux select-pane -U -t pitted
# tmux set -t pitted mouse on
# tmux attach -t pitted

# i'm gonna find a way to run multiple docker instances in one line?
# tmux new-session -d -s pitted
# tmux split-window -t pitted 'docker run -p 8081:8080 --net=mynet --ip=10.0.0.21 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080" -e "IPPORT"="10.0.0.21:8080" kvs'
# tmux select-layout -t pitted main-horizontal
# tmux split-window -h -p 66 -t pitted  'docker run -p 8082:8080 --net=mynet --ip=10.0.0.22 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080" -e "IPPORT"="10.0.0.22:8080" kvs'

# # tmux select-layout -t pitted tiled
# tmux select-pane -U -t pitted
# tmux set -t pitted mouse on
# tmux attach -t pitted
