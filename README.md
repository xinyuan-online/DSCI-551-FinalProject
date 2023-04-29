# DSCI-551-FinalProject
## EDFS - A Hadoop HDFS Emulation

### How to use:
There are two ways for setting up EDFS. </br>

#### **Single machine setup**:
1. git clone and cd into the cloned directory</br>
2. (optional) edit configuration files in conf/, adding datanodes or configure if you like</br>
3. (optional) chmod 700 ./bin/*</br>
4. (optional) add the line: "export PATH=$PATH:{path_to_repo}/bin" to .bash_rc</br>
5. (if you modified bash_rc)restart the shell</br>

6. start-dfs (or cd to the directory and use bin/start-dfs if not modified PATH)</br>
now you have started the namenode and datanodes on this machine</br>

7. open a new shell</br>
8. edfs [-command] [arguments] (or bin/edfs if not modified PATH)</br>
9. when you want to terminate the servers, switch back to the server session and ctrl-c</br>

10. (optional for a web-ui) python3 ./web_ui.py local</br>
Note: [mode] can be one of the following: "remote", "local", or arbitary hostname, which will assign the corresponding option to the hostname of the frontend web client</br>

#### **Remote machine setup**:
1. git clone on every machine you want to use as namenode and datanode, or client
2. Edit configuration file on machine used as server.
3. Follow the step 3-6 above
4. On server side, run python3 ./web_ui.py remote
5. On client side, configure the configuration to reflect the remote hostname
6. Now you can use edfs, or the web-ui on {server-hostname}:5000 in your browser to view edfs


