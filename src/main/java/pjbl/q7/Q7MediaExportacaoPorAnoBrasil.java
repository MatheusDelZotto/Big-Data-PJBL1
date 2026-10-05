package pjbl.q7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import pjbl.writables.MediaWritable;

import java.io.IOException;

// Questão 7: valor médio das transações por ano, somente exportações (Export) do Brasil.
// Usa Combiner e o writable customizado MediaWritable (soma, qtd)
public class Q7MediaExportacaoPorAnoBrasil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/q7");

        // criacao do job e seu nome
        Job j = new Job(c, "mediaExportacaoBrasil");

        //1. registro das classes
        j.setJarByClass(Q7MediaExportacaoPorAnoBrasil.class);
        j.setMapperClass(MapForMediaExportacao.class);
        j.setReducerClass(ReduceForMediaExportacao.class);
        j.setCombinerClass(CombineForMediaExportacao.class);

        //2. Definição dos tipos de saída
        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(MediaWritable.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(DoubleWritable.class);

        //3. Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4. Lança o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForMediaExportacao extends Mapper<LongWritable, Text, IntWritable, MediaWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            //retira o cabeçalho
            if (linha.startsWith("country_or_area")) { return; }

            String[] col = linha.split(";");

            //trata dados faltantes: linha incompleta, ano ou preço vazio
            if (col.length < 10 || col[1].isEmpty() || col[5].isEmpty()) { return; }

            //descarta as linhas TOTAL (ALL COMMODITIES): são a soma de todas as transações
            //do país no ano, não uma transação; mantê-las contaria os valores em dobro
            if (col[2].equals("TOTAL")) { return; }

            if (col[0].equals("Brazil") && col[4].equals("Export")) {
                int ano = Integer.parseInt(col[1]);
                double preco = Double.parseDouble(col[5]);

                con.write(new IntWritable(ano), new MediaWritable(preco, 1));
            }
        }
    }

    // combiner: soma parcial (soma, qtd) na saída de cada map; a média só é calculada no reduce
    public static class CombineForMediaExportacao extends Reducer<IntWritable, MediaWritable, IntWritable, MediaWritable> {
        public void reduce(IntWritable key, Iterable<MediaWritable> values, Context con)
                throws IOException, InterruptedException {
            double somaPreco = 0;
            int somaQtd = 0;

            for (MediaWritable v : values) {
                somaPreco += v.getSoma();
                somaQtd += v.getQtd();
            }

            con.write(key, new MediaWritable(somaPreco, somaQtd));
        }
    }

    public static class ReduceForMediaExportacao extends Reducer<IntWritable, MediaWritable, IntWritable, DoubleWritable> {
        public void reduce(IntWritable key, Iterable<MediaWritable> values, Context con)
                throws IOException, InterruptedException {
            double somaPreco = 0;
            int somaQtd = 0;

            for (MediaWritable v : values) {
                somaPreco += v.getSoma();
                somaQtd += v.getQtd();
            }

            double media = somaPreco / somaQtd;
            con.write(key, new DoubleWritable(media));
        }
    }
}
