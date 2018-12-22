package com;


import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class ConnectionHadoop {
	public static void main(String[] args) {
		PrivilegedExceptionAction<Void> pea = new PrivilegedExceptionAction<Void>() {   // 무명클래스

			@Override
			public Void run() throws Exception {
				Configuration config = new Configuration();
				config.set("fs.defaultFS", "hdfs://192.168.0.154:9000/user/bdi");  // 자기 리눅스 아이피, 앞에껀 키값
				config.setBoolean("dfs.suppourt.append", true);
				
				FileSystem fs = FileSystem.get(config);
				
				Path upFileName = new Path("word.txt");		// 워드라는 파일을
				Path logFileName = new Path("word.log");
				if(fs.exists(upFileName)) {			// 파일이 있다면
					fs.delete(upFileName,true);		// 지우고나서
					fs.delete(logFileName,true);
				}
				FSDataOutputStream fsdo = fs.create(upFileName);		// 만들고
				fsdo.writeUTF("hi hi hi hey hey lol start hi");			// 내용 넣는다
				fsdo.close();
				
				Path dirName = new Path("/user/bdi");				// /user/bdi 안에 내용을 찾는다.
				FileStatus[] files = fs.listStatus(dirName);		// 출력
				for(FileStatus file:files) {
					System.out.println(file);
				}
				return null;
			}
		};
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bdi");
		try {
			ugi.doAs(pea);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
