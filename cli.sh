#!/bin/bash
basepath=$(cd `dirname $0`; pwd)
cd $basepath

export LC_ALL="en_US.utf8" 2>/dev/null
error() {
    echo -e `date`"\033[31m $1 \033[0m"
    exit 1
}

info() {
    echo -e `date`"\033[36m $1 \033[0m"
}

notice() {
    echo -e `date`"\033[32m $1 \033[0m"
}

warn() {
    echo -e `date`"\033[33m $1 \033[0m"
}

mkdir -p .cli

which ipython3 &> /dev/null
if [[ $? -ne 0 ]]; then
    error "NO ipython3 find, please install python3 and ipython manually before using idaas cli"
fi

server=`cat $basepath/config.ini |grep "server"|awk -F ' ' '{print $3}'`
ak=`cat $basepath/config.ini |grep "ak"|grep -v \#`
if [[ $? -eq 0 ]]; then
    info "connecting remote server: https://cloud.tapdata.net ..."
else
    info "connecting remote server: $server ..."
fi

info "Welcome to TapData Live Data Platform, Enjoy Your Data Trip !"
cp ipython_config.py .cli/ipython_config.py
ipython3 --no-banner --profile-dir=$basepath/.cli --profile=ipython_config -i $basepath/cli.py
