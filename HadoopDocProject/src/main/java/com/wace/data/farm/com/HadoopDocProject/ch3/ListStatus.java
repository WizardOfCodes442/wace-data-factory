//Listing Files 

//Finding information on a single file or directory i useful 
//but you also often need to be able to list the the contents of a directory 
//That's what FileSystem's listSttus() methods are for 
//public FileStatus[] listStatus(path f) throws IOException
//public FileStatus[] listStatus(Path f, PathFilter) throws IOException
//public FileStatus[] listStatus(path[] files) throws IOException
//public FileStatus[] listStatus(Path[] files, PathFilter filter) throws IOException

//When the argument is a file, the simplest variant returns an array of 
//FileStatus objects of length 1.
//when the argument is a directory, it returns zero or more FileStatus objects

//If you specify an array of paths , the result is a shortcut for calling the equivalent singlepath
//listStatus() method for each path in turn and acumulating the FileStatus object in a single array.
//This can be useful for building a list of input files to procxess from distinct parts of the filesystem tree

//Note the use of stat2Paths() in Hadoop's FileUtil for turning an array of 
//FileStatus objects into an array of Path objects 

package com.wace.data.farm.com.HadoopDocProject.ch3;

import java.net.URI;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;

public class ListStatus {

		public static void main(String [] args) throws Exception {
			String uri = args[0];
			
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			
			Path[]  paths  = new Path[args.length];
			for (int i = 0; i < paths.length; i++) {
				paths[i] = new Path(args[i]);
			}
			
			FileStatus[] status = fs.listStatus(paths);
			Path[] listedPaths = FileUtil.stat2Paths(status);
			for (Path p: listedPaths) {
				System.out.println(p);
			}
		}
		
}
