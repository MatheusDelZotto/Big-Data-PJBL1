package pjbl.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// chave composta (ano, país): por ser chave precisa ser WritableComparable,
// pois o Hadoop ordena as chaves no sort/shuffle
public class AnoPaisWritable implements WritableComparable<AnoPaisWritable> {

    private int ano;
    private String pais;

    public AnoPaisWritable() {
    }

    public AnoPaisWritable(int ano, String pais) {
        this.ano = ano;
        this.pais = pais;
    }

    public int getAno() {
        return ano;
    }

    public void setAno(int ano) {
        this.ano = ano;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    // ordena por ano e, no mesmo ano, por país
    @Override
    public int compareTo(AnoPaisWritable o) {
        if (this.ano != o.ano) {
            return Integer.compare(this.ano, o.ano);
        }
        return this.pais.compareTo(o.pais);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(ano);
        out.writeUTF(pais);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ano = in.readInt();
        pais = in.readUTF();
    }

    @Override
    public String toString() {
        return ano + "\t" + pais;
    }
}
