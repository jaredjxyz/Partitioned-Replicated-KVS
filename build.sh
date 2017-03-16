# build.sh kills all docker containers, rebuilds container when you edit source
# running containers
docker kill $(docker ps -q)
docker build -t kvs .

# python test.py


# i'm gonna find a way to run multiple docker instances in one line?
tmux new-session -d -s pitted


tmux split-window -t pitted 'docker run -p 8081:8080 --net=mynet --ip=10.0.0.21 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080" -e "IPPORT"="10.0.0.21:8080" kvs'
tmux select-layout -t pitted main-horizontal
tmux split-window -h -p 66 -t pitted  'docker run -p 8082:8080 --net=mynet --ip=10.0.0.22 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080" -e "IPPORT"="10.0.0.22:8080" kvs'
tmux split-window -h -t pitted -p 50 'docker run -p 8083:8080 --net=mynet --ip=10.0.0.23 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080" -e "IPPORT"="10.0.0.23:8080" kvs'
tmux select-pane -L -t pitted
tmux select-pane -L -t pitted
tmux split-window -v -t pitted 'docker run -p 8084:8080 --net=mynet --ip=10.0.0.24 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080" -e "IPPORT"="10.0.0.24:8080" kvs'
tmux select-pane -R -t pitted
tmux split-window -v -t pitted 'docker run -p 8085:8080 --net=mynet --ip=10.0.0.25 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080" -e "IPPORT"="10.0.0.25:8080" kvs'
tmux select-pane -R -t pitted
tmux split-window -v -t pitted 'docker run -p 8086:8080 --net=mynet --ip=10.0.0.26 -e K=2 -e VIEW="10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080,10.0.0.26:8080" -e "IPPORT"="10.0.0.26:8080" kvs'
tmux select-pane -U -t pitted

# tmux select-layout -t pitted tiled
tmux select-pane -U -t pitted
tmux set -t pitted mouse on
tmux attach -t pitted
