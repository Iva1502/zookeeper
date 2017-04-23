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
import java.io.Closeable;
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
		while (true)
		{
		HashMap<String, String> groups_cache = new HashMap<String, String>();	
		System.out.println("Choose one of the following options by entering the proper number:\n 1.Create a group\n 2.Join a group\n 3.List all current groups\n");
		Scanner scanner_choice = new Scanner(System.in); 
		String choice = scanner_choice.next();
		//if (choice != null)
			//scanner_choice.close();
		switch (choice) {
			case "1":
				System.out.println("Enter the name of your group:\n");
				Scanner scanner_gname = new Scanner(System.in); 
				String gname = scanner_gname.next();
				CuratorFramework zkc = CuratorFrameworkFactory.newClient("localhost:2181",
						new ExponentialBackoffRetry(1000, 5));
				zkc.start();
				List<String> group_names=zkc.getChildren().forPath("/groups");
				int i;
				for(i=0; i<group_names.size();i++){
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
					} catch (InterruptedException e) {
						System.out.println(e.toString());
					}
				}
				else
					System.out.println("A group with that name already exists!");
				
				break;
			case "2":
				System.out.println("Enter the name of the group you want to join:\n");
				Scanner scanner_joinname = new Scanner(System.in); 
				String joinname = scanner_joinname.next();
				String real_name=null;
				CuratorFramework zkc1 = CuratorFrameworkFactory.newClient("localhost:2181",
						new ExponentialBackoffRetry(1000, 5));
				zkc1.start();
				
					if (groups_cache.get(joinname)!=null)
						real_name=groups_cache.get(joinname);
					else{
						List<String> children=zkc1.getChildren().forPath("/groups");
						for(int j=0; j<children.size();j++){
							if(Objects.equals(children.get(j).substring(40),joinname)==true){
								real_name=children.get(j);
								break;
							}	
						}
					}
					
				if(real_name==null)
					System.out.println("No such group exists");
				else{
					System.out.println("Enter your unique ID:");
					Scanner scanner_id = new Scanner(System.in); 
					String id = scanner_id.next();
					System.out.println("Enter your payload:");
					Scanner scanner_payload = new Scanner(System.in); 
					String payload = scanner_payload.next();
					byte[] b=payload.getBytes();
					GroupMember gm = new GroupMember(zkc1, "/groups/" + real_name, id, b);
					gm.start();
					while (true)
					{
						int temp=0;
						System.out.println("Choose one of the following options by entering the proper number:\n 1.Leave the group\n 2.List all current group members\n");
						Scanner scanner_choice2 = new Scanner(System.in); 
						String choice2 = scanner_choice2.next();
						switch (choice2) {
							case "1":
								temp=1;
								gm.close();
								break;
							case "2":
								//Map<String, byte[]> members = new Map<String, byte[]>();
								//members=gm.getCurrentMembers().toString();
								for(String element:gm.getCurrentMembers().keySet()){
									if(element.length()>40)
									{
										System.out.println(element.substring(40));	
									}
								}
								//System.out.println(gm.getCurrentMembers().keySet());
								break;		
						}
					if (temp==1)
						break;
					}	
				}
				
				break;
			case "3":
				
				CuratorFramework zkc2 = CuratorFrameworkFactory.newClient("localhost:2181",
						new ExponentialBackoffRetry(1000, 5));
				zkc2.start();
				List<String> children=zkc2.getChildren().forPath("/groups");
				for(int j=0; j<children.size();j++){
					System.out.println(children.get(j).substring(40));
					if(groups_cache.get(children.get(j).substring(40))==null){
						groups_cache.put(children.get(j).substring(40), children.get(j));
					}	
				}
				
				break;
		}
		}
		
		/*try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

	}
}