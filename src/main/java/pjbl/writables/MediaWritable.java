package pjbl.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// writable customizado para calcular média: guarda a soma dos valores e a quantidade
public class MediaWritable implements Writable {

    private double soma;
    private int qtd;

    public MediaWritable() {
    }

    public MediaWritable(double soma, int qtd) {
        this.soma = soma;
        this.qtd = qtd;
    }

    public double getSoma() {
        return soma;
    }

    public void setSoma(double soma) {
        this.soma = soma;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    // escreve os atributos
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeInt(qtd);
    }

    // le os campos, ou seja, lê os atributos (na mesma ordem em que foram escritos)
    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        qtd = in.readInt();
    }
}
