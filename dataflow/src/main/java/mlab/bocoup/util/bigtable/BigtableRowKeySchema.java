package mlab.bocoup.util.bigtable;

public class BigtableRowKeySchema implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String name;
	public Integer size;
	
	
	public BigtableRowKeySchema(String name, Integer size) {
		this.name = name;
		this.size = size;
	}
	
	public String getName() {
		return name;
	}
	
	public Integer getSize() {
		return size;
	}
	
}