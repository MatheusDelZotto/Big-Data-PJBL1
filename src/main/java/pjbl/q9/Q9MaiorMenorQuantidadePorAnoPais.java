package pjbl.q9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import pjbl.writables.AnoPaisWritable;
import pjbl.writables.MinMaxWritable;

import java.io.IOException;

// Questão 9: transação com o maior e o menor preço (com base na coluna amount/quantity), por ano e país.
// Chave: AnoPaisWritable (WritableComparable customizado). Usa Combiner.
public class Q9MaiorMenorQuantidadePorAnoPais {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/q9");

        // criacao do job e seu nome
        Job j = new Job(c, "maiorMenorPorAnoPais");

        //1. registro das classes
        j.setJarByClass(Q9MaiorMenorQuantidadePorAnoPais.class);
        j.setMapperClass(MapForAnoPais.class);
        j.setReducerClass(ReduceForAnoPais.class);
        j.setCombinerClass(CombineForAnoPais.class);

        //2. Definição dos tipos de saída
        j.setMapOutputKeyClass(AnoPaisWritable.class);
        j.setMapOutputValueClass(MinMaxWritable.class);
        j.setOutputKeyClass(AnoPaisWritable.class);
        j.setOutputValueClass(MinMaxWritable.class);

        //3. Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4. Lança o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAnoPais extends Mapper<LongWritable, Text, AnoPaisWritable, MinMaxWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            //retira o cabeçalho
            if (linha.startsWith("country_or_area")) { return; }

            String[] col = linha.split(";");

            //trata dados faltantes: linha incompleta, país, ano ou amount (quantity) vazio
            if (col.length < 10 || col[0].isEmpty() || col[1].isEmpty() || col[8].isEmpty()) { return; }

            //descarta as linhas TOTAL (ALL COMMODITIES): são a soma de todas as transações
            //do país no ano, não uma transação; mantê-las contaria os valores em dobro
            if (col[2].equals("TOTAL")) { return; }

            int ano = Integer.parseInt(col[1]);
            double amount = Double.parseDouble(col[8]);

            con.write(new AnoPaisWritable(ano, col[0]), new MinMaxWritable(amount, col[3], col[4]));
            //((2016, Afghanistan), (51, Sheep, live, Export))
        }
    }

    public static class CombineForAnoPais extends Reducer<AnoPaisWritable, MinMaxWritable, AnoPaisWritable, MinMaxWritable> {
        public void reduce(AnoPaisWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {
            //percorre os pares (menor, maior) e guarda o menor dos menores e o maior dos maiores
            //em caso de empate no valor, fica a mercadoria que vem primeiro em ordem alfabética,
            //assim o resultado não depende da ordem em que os valores chegam
            boolean primeiro = true;
            double menorValor = 0, maiorValor = 0;
            String menorMercadoria = "", menorFluxo = "", maiorMercadoria = "", maiorFluxo = "";

            for (MinMaxWritable v : values) {
                if (primeiro
                        || v.getMenorValor() < menorValor
                        || (v.getMenorValor() == menorValor && v.getMenorMercadoria().compareTo(menorMercadoria) < 0)) {
                    menorValor = v.getMenorValor();
                    menorMercadoria = v.getMenorMercadoria();
                    menorFluxo = v.getMenorFluxo();
                }
                if (primeiro
                        || v.getMaiorValor() > maiorValor
                        || (v.getMaiorValor() == maiorValor && v.getMaiorMercadoria().compareTo(maiorMercadoria) < 0)) {
                    maiorValor = v.getMaiorValor();
                    maiorMercadoria = v.getMaiorMercadoria();
                    maiorFluxo = v.getMaiorFluxo();
                }
                primeiro = false;
            }

            con.write(key, new MinMaxWritable(menorValor, menorMercadoria, menorFluxo,
                    maiorValor, maiorMercadoria, maiorFluxo));
        }
    }

    public static class ReduceForAnoPais extends Reducer<AnoPaisWritable, MinMaxWritable, AnoPaisWritable, MinMaxWritable> {
        public void reduce(AnoPaisWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {
            //percorre os pares (menor, maior) e guarda o menor dos menores e o maior dos maiores
            //em caso de empate no valor, fica a mercadoria que vem primeiro em ordem alfabética,
            //assim o resultado não depende da ordem em que os valores chegam
            boolean primeiro = true;
            double menorValor = 0, maiorValor = 0;
            String menorMercadoria = "", menorFluxo = "", maiorMercadoria = "", maiorFluxo = "";

            for (MinMaxWritable v : values) {
                if (primeiro
                        || v.getMenorValor() < menorValor
                        || (v.getMenorValor() == menorValor && v.getMenorMercadoria().compareTo(menorMercadoria) < 0)) {
                    menorValor = v.getMenorValor();
                    menorMercadoria = v.getMenorMercadoria();
                    menorFluxo = v.getMenorFluxo();
                }
                if (primeiro
                        || v.getMaiorValor() > maiorValor
                        || (v.getMaiorValor() == maiorValor && v.getMaiorMercadoria().compareTo(maiorMercadoria) < 0)) {
                    maiorValor = v.getMaiorValor();
                    maiorMercadoria = v.getMaiorMercadoria();
                    maiorFluxo = v.getMaiorFluxo();
                }
                primeiro = false;
            }

            con.write(key, new MinMaxWritable(menorValor, menorMercadoria, menorFluxo,
                    maiorValor, maiorMercadoria, maiorFluxo));
        }
    }
}
