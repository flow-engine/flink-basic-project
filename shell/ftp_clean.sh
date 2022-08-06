#!/bin/bash -xe

branch=$1
# 去掉 release/ 前缀
version=${branch#release/}
# / 替换成 -
ftp_version=${version//\//-}
echo "branch is ${branch}"
echo "version is ${version}"
echo "ftp_version is ${ftp_version}"

if [ ! -n "${ftp_version}" ]; then
  echo "will set ftp_version: develop"
  ftp_version="develop"
fi

jars=`curl -s ftp://ftp.4pd.io/pub/pws/hadoop/${ftp_version}/flink/ | awk '{print $9}' | grep jar | grep csp-flink-operator`
echo "list all csp jars: ${jars}"
for i in ${jars}; do \
	echo "will delete $i"; \
#	curl ftp://ftp.4pd.io/pub/pws/hadoop/${ftp_version}/flink/$i -Q "-DELE ${i}";\
done
curl -s ftp://ftp.4pd.io/pub/pws/hadoop/${ftp_version}/flink/
echo "deleting done."