package sjoin;

import java.util.Arrays;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

@SuppressWarnings("unchecked")
public class Geometry extends GenericWritable {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
            PointWritable.class,
            RectangleWritable.class
        };
    }
    
    public Geometry() {
    }

    public Geometry(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }

    @Override
    public String toString() {
        return "Geometry [getTypes()=" + Arrays.toString(getTypes()) + "]";
    }
}
