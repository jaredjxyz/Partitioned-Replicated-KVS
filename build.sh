# build.sh kills all docker containers, rebuilds container when you edit source
# running containers
docker kill $(docker ps -q)
docker build -t kvs .

# python test.py


# i'm gonna find a way to run multiple docker instances in one line?
tmux new-session -d -s pitted
tmux split-window -t pitted 'docker run -p 8080:8080 --net=mynet --ip=10.0.0.20 -e VIEW="10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080" -e "IPPORT"="10.0.0.20:8080" kvs'
tmux split-window -t pitted 'docker run -p 8081:8080 --net=mynet --ip=10.0.0.21 -e VIEW="10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080" -e "IPPORT"="10.0.0.21:8080" kvs'
tmux split-window -t pitted 'docker run -p 8082:8080 --net=mynet --ip=10.0.0.22 -e VIEW="10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080" -e "IPPORT"="10.0.0.22:8080" kvs'
tmux select-layout  -t pitted main-horizontal
tmux select-pane -U -t pitted
tmux set -t pitted mouse on
tmux attach -t pitted
