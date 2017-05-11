package org.apache.curator.framework.recipes.nodes;

//package org.apache.curator.framework.recipes.nodes;
import com.google.common.base.Preconditions;
import org.apache.zookeeper.CreateMode;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.Set;

//import com.netflix.curator.retry;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Group membership management. Adds this instance into a group and keeps a
 * cache of members in the group
 */
public class GroupMember implements Closeable {
	private final PersistentNode pen;
	private final PathChildrenCache cache;
	private final String thisId;

	/**
	 * @param client
	 *            client
	 * @param membershipPath
	 *            the path to use for membership
	 * @param thisId
	 *            ID of this group member. MUST be unique for the group
	 */
	public GroupMember(CuratorFramework client, String membershipPath, String thisId) {
		this(client, membershipPath, thisId, CuratorFrameworkFactory.getLocalAddress());
	}

	/**
	 * @param client
	 *            client
	 * @param membershipPath
	 *            the path to use for membership
	 * @param thisId
	 *            ID of this group member. MUST be unique for the group
	 * @param payload
	 *            the payload to write in our member node
	 */
	public GroupMember(CuratorFramework client, String membershipPath, String thisId, byte[] payload) {
		this.thisId = Preconditions.checkNotNull(thisId, "thisId cannot be null");
		cache = newPathChildrenCache(client, membershipPath);
		pen = newEphemeralNode(client, membershipPath, thisId, payload);
	}

	/**
	 * Start the group membership. Register thisId as a member and begin caching
	 * all members
	 */
	public void start() {
		pen.start();
		try {
			pen.waitForInitialCreate(3, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			System.out.println(e.toString());
		}
		try {
			cache.start();
		} catch (Exception e) {
			ThreadUtils.checkInterrupted(e);
			Throwables.propagate(e);
		}
	}

	/**
	 * Change the data stored in this instance's node
	 *
	 * @param data
	 *            new data (cannot be null)
	 */
	public void setThisData(byte[] data) {
		try {
			pen.setData(data);
		} catch (Exception e) {
			ThreadUtils.checkInterrupted(e);
			Throwables.propagate(e);
		}
	}

	/**
	 * Have thisId leave the group and stop caching membership
	 */
	@Override
	public void close() {
		CloseableUtils.closeQuietly(cache);
		CloseableUtils.closeQuietly(pen);
	}

	/**
	 * Return the current view of membership. The keys are the IDs of the
	 * members. The values are each member's payload
	 *
	 * @return membership
	 */
	public Map<String, byte[]> getCurrentMembers() {
		ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
		boolean thisIdAdded = false;
		for (ChildData data : cache.getCurrentData()) {
			String id = idFromPath(data.getPath());
			thisIdAdded = thisIdAdded || id.equals(thisId);
			builder.put(id, data.getData());
		}
		if (!thisIdAdded) {
			builder.put(thisId, pen.getData()); // this instance is always a
												// member
		}
		return builder.build();
	}

	/**
	 * Given a full ZNode path, return the member ID
	 *
	 * @param path
	 *            full ZNode path
	 * @return id
	 */
	public static String idFromPath(String path) {
		return ZKPaths.getNodeFromPath(path);
	}

	protected static PersistentNode newPersistentNode(CuratorFramework client, String membershipPath, String thisId,
			byte[] payload) {
		return new PersistentNode(client, CreateMode.PERSISTENT, true, ZKPaths.makePath(membershipPath, thisId),
				payload);
	}

	protected static PersistentNode newEphemeralNode(CuratorFramework client, String membershipPath, String thisId,
			byte[] payload) {
		return new PersistentNode(client, CreateMode.EPHEMERAL, true, ZKPaths.makePath(membershipPath, thisId),
				payload);
	}

	protected static PathChildrenCache newPathChildrenCache(CuratorFramework client, String membershipPath) {
		return new PathChildrenCache(client, membershipPath, true);
	}

	public static void main(String args[]) throws Exception {

		HashMap<String, String> groups_cache = new HashMap<String, String>();
		if (Objects.equals(args[0],"list")==true)
		{

			String connection_string=null;
			int count=Integer.parseInt(args[1]);
			while (count>0){
				connection_string += "localhost:" + Integer.toString(2180+count) + ",";
				count--;
			}
			connection_string=connection_string.substring(0,connection_string.length()-1);
			
			CuratorFramework zkc2 = CuratorFrameworkFactory.newClient(connection_string,
					new ExponentialBackoffRetry(1000, 5));
					zkc2.start();
					List<String> children=zkc2.getChildren().forPath("/groups");
					for(int j=0; j<children.size();j++){
					System.out.println(children.get(j).substring(40));
					if(groups_cache.get(children.get(j).substring(40))==null){
					groups_cache.put(children.get(j).substring(40), children.get(j));
					}
					}
		}
		if (Objects.equals(args[0],"close")==true)
		{
			String connection_string=null;
			int count=Integer.parseInt(args[1]);
			while (count>0){
				connection_string += "localhost:" + Integer.toString(2180+count) + ",";
				count--;
			}
			connection_string=connection_string.substring(0,connection_string.length()-1);
			
			CuratorFramework zkc2 = CuratorFrameworkFactory.newClient(connection_string,
					new ExponentialBackoffRetry(1000, 5));
					zkc2.start();
					List<String> children=zkc2.getChildren().forPath("/groups");
					for(int j=0; j<children.size();j++){
						List<String> grandchildren=zkc2.getChildren().forPath("/groups/"+children.get(j));
						for(int l=0; l<grandchildren.size();l++){
							String path="/groups/"+children.get(j)+"/"+grandchildren.get(l);
							zkc2.delete().deletingChildrenIfNeeded().forPath(path);
						}
					}
		}
		else{
			List<String> records = new ArrayList<String>();
			try {
				BufferedReader reader = new BufferedReader(new FileReader(args[1]));
				String line;
				while ((line = reader.readLine()) != null) {
					records.add(line);
				}
				reader.close();
			} catch (Exception e) {
				System.err.format("Exception occurred trying to read the file");
				e.printStackTrace();
			}
			
			if (Objects.equals(args[0],"start")==true)
			{
				if (records.size()<Integer.parseInt(args[2]))
					System.out.println("Not enough data in the file" );
				else
				{
					for (int i = 0; i < Integer.parseInt(args[2]); i++) {
						String real_name = null;
						String connection_string=null;
						int count=Integer.parseInt(args[3]);
						while (count>0){
							connection_string += "localhost:" + Integer.toString(2180+count) + ",";
							count--;
						}
						connection_string=connection_string.substring(0,connection_string.length()-1);
					
						CuratorFramework zkc1 = CuratorFrameworkFactory.newClient(connection_string,
								new ExponentialBackoffRetry(1000, 5));
						zkc1.start();
						List<String> temp = Arrays.asList(records.get(i).split(";"));
						List<String> children = zkc1.getChildren().forPath("/groups");
						for (int j = 0; j < children.size(); j++) {
							if (Objects.equals(children.get(j).substring(40), temp.get(0)) == true) {
								real_name = children.get(j);
								break;
							}
						}
						if (real_name == null)
							System.out.println("No such group exists");
						else {
							GroupMember gm = new GroupMember(zkc1, "/groups/" + real_name, temp.get(1), temp.get(2).getBytes());
							gm.start();
						}
					}
				}
			}
			if (Objects.equals(args[0],"create")==true)
			{
				String connection_string=null;
				int count=Integer.parseInt(args[2]);
				while (count>0){
					connection_string += "localhost:" + Integer.toString(2180+count) + ",";
					count--;
				}
				connection_string=connection_string.substring(0,connection_string.length()-1);
				
				for (int p=0; p<records.size();p++)
				{
					String gname=records.get(p);
					CuratorFramework zkc = CuratorFrameworkFactory.newClient(connection_string,
							new ExponentialBackoffRetry(1000, 5));
					zkc.start();
					List<String> group_names=zkc.getChildren().forPath("/groups");
					int i;
					for(i=0; i<group_names.size();i++)
					{
						if (Objects.equals(group_names.get(i).substring(40),gname)==true)
							break;
					}
					if (i==group_names.size()){
						String gr="group";
						byte[] b_group=gr.getBytes();
						PersistentNode group_node = newPersistentNode(zkc,"/groups",gname , b_group);
						group_node.start();
						try {
							group_node.waitForInitialCreate(3, TimeUnit.SECONDS);
						} catch (InterruptedException e)
						{
							System.out.println(e.toString());
						}
					}
					else
						System.out.println("A group with that name already exists!");
				}
			}
			
		}
	
		 /*try { Thread.sleep(10000); } catch (InterruptedException e) { //
		 * TODO Auto-generated catch block e.printStackTrace(); }
		 */

	}
}