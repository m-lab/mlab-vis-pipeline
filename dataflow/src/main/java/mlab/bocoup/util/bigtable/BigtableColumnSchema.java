package mlab.bocoup.util.bigtable;

public class BigtableColumnSchema implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String name;
	public String type;
	public String family;

	public BigtableColumnSchema(String name, String type, String family) {
		this.name = name;
		this.type = type;
		this.family = family;
	}
	
	public String getName() {
		return name;
	}
	
	public String getType() { 
		return type;
	}
	
	public String getFamily() {
		return family;
	}
	
		
}