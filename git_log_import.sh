#!/bin/bash

#arguments
user=${1}
connection=${1}
token=${3}
owner=${4}
repo=${5}
pull_date=${6}
archive_hdfs_path=${7}

set -vx

kinit -kt $user $connection

load_date=`date -d "today" '+%Y%m%d'`

#call Githup REST API and pull the commit events and wirte into hdfs

curl -H "Accept: application/vnd.github+json" -H "Authorization: token ${token}" "https://api.GitHub.com/repos/${owner}/${repo}/issues/events?&since=${pull_date}" > git_commits_${load_date}_log.json

hadoop fs -put git_commits_${load_date}_log.json ${archive_hdfs_path}

