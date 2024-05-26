
package com.ignite.project.demo.dim;

import lombok.Data;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.util.Objects;

@Data
public class DimStore implements Serializable {
    /** Primary key. */
    @QuerySqlField(index = true)
    private int id;

    /** Store name. */
    @QuerySqlField(index = true, orderedGroups = @QuerySqlField.Group(name = "DIM_STORE_IDX", order = 0))
    private String name;

    /** Zip code. */
    @QuerySqlField
    private String zip;

    /** Address. */
    @QuerySqlField
    private String addr;

    public DimStore(){}

    /**
     * Constructs a store instance.
     *
     * @param id Store ID.
     * @param name Store name.
     * @param zip Store zip code.
     * @param addr Store address.
     */
    public DimStore(int id, String name, String zip, String addr) {
        this.id = id;
        this.name = name;
        this.zip = zip;
        this.addr = addr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DimStore dimStore = (DimStore) o;
        return id == dimStore.id && name.equals(dimStore.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "DimStore [id=" + id +
            ", name=" + name +
            ", zip=" + zip +
            ", addr=" + addr + ']';
    }
}
