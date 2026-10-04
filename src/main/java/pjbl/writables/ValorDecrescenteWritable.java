package pjbl.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// chave usada na Q8 para ordenar os valores do maior para o menor:
// o compareTo é invertido, então o sort/shuffle do Hadoop entrega as chaves em ordem decrescente
public class ValorDecrescenteWritable implements WritableComparable<ValorDecrescenteWritable> {

    private long valor;

    public ValorDecrescenteWritable() {
    }

    public ValorDecrescenteWritable(long valor) {
        this.valor = valor;
    }

    public long getValor() {
        return valor;
    }

    public void setValor(long valor) {
        this.valor = valor;
    }

    @Override
    public int compareTo(ValorDecrescenteWritable o) {
        return Long.compare(o.valor, this.valor); // invertido -> decrescente
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(valor);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        valor = in.readLong();
    }

    @Override
    public String toString() {
        return String.valueOf(valor);
    }
}
