package pjbl.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

// writable customizado que guarda a transação de menor valor e a de maior valor
// (valor + mercadoria + fluxo de cada uma)
public class MinMaxWritable implements Writable {

    private double menorValor;
    private String menorMercadoria;
    private String menorFluxo;

    private double maiorValor;
    private String maiorMercadoria;
    private String maiorFluxo;

    public MinMaxWritable() {
    }

    // no map cada transação é ao mesmo tempo a menor e a maior
    public MinMaxWritable(double valor, String mercadoria, String fluxo) {
        this(valor, mercadoria, fluxo, valor, mercadoria, fluxo);
    }

    public MinMaxWritable(double menorValor, String menorMercadoria, String menorFluxo,
                          double maiorValor, String maiorMercadoria, String maiorFluxo) {
        this.menorValor = menorValor;
        this.menorMercadoria = menorMercadoria;
        this.menorFluxo = menorFluxo;
        this.maiorValor = maiorValor;
        this.maiorMercadoria = maiorMercadoria;
        this.maiorFluxo = maiorFluxo;
    }

    public double getMenorValor() {
        return menorValor;
    }

    public String getMenorMercadoria() {
        return menorMercadoria;
    }

    public String getMenorFluxo() {
        return menorFluxo;
    }

    public double getMaiorValor() {
        return maiorValor;
    }

    public String getMaiorMercadoria() {
        return maiorMercadoria;
    }

    public String getMaiorFluxo() {
        return maiorFluxo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(menorValor);
        out.writeUTF(menorMercadoria);
        out.writeUTF(menorFluxo);
        out.writeDouble(maiorValor);
        out.writeUTF(maiorMercadoria);
        out.writeUTF(maiorFluxo);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        menorValor = in.readDouble();
        menorMercadoria = in.readUTF();
        menorFluxo = in.readUTF();
        maiorValor = in.readDouble();
        maiorMercadoria = in.readUTF();
        maiorFluxo = in.readUTF();
    }

    // formato usado no arquivo de saída
    @Override
    public String toString() {
        return String.format(Locale.US, "MENOR: %.2f (%s - %s)\tMAIOR: %.2f (%s - %s)",
                menorValor, menorMercadoria, menorFluxo,
                maiorValor, maiorMercadoria, maiorFluxo);
    }
}
