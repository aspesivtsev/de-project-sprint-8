HOST_VM=178.154.222.65  #укажите здесь хост вашей вм.#!/bin/bash

USER_VM=yc-user
IMAGE_VM=$(ssh -i ssh_key $USER_VM@$HOST_VM docker ps --format '{{.Names}}')
echo USER_VM $USER_VM
echo HOST_VM $HOST_VM
echo IMAGE_VM $IMAGE_VM
scp -ri ssh_key data_to_vm $USER_VM@$HOST_VM:~/
ssh -i ssh_key $USER_VM@$HOST_VM "
    docker cp ~/data_to_vm $IMAGE_VM:/lessons/
"


#chmod +x [scp-files.sh](http://scp-files.sh)
#./scp-files.sh
