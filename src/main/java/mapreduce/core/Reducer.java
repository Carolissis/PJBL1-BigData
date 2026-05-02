package mapreduce.core;

import java.io.IOException;
import java.util.List;

public abstract class Reducer<KIN extends Writable, VIN extends Writable,
                              KOUT extends Writable, VOUT extends Writable> {
    public abstract void reduce(KIN key, List<VIN> values, Context<KOUT, VOUT> context)
            throws IOException, InterruptedException;
}
