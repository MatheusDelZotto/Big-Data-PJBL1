package pjbl.q8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import pjbl.writables.ValorDecrescenteWritable;

import java.io.IOException;

// Questão 8: valor máximo das transações (trade_usd) por ano no Brasil,
// ordenado do maior valor para o menor. Usa concatenação de jobs:
//   job 1: calcula o máximo de cada ano            -> (ano, máximo)
//   job 2: lê a saída do job 1 e ordena pelo valor -> chave ValorDecrescenteWritable
public class Q8MaximoPorAnoBrasilOrdenado {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // saida do job 1 / entrada do job 2
        Path intermediate = new Path("output/q8_intermediario");

        // arquivo de saida
        Path output = new Path("output/q8");

        //configuração do job1
        Job j1 = new Job(c, "maximoPorAno");

        j1.setJarByClass(Q8MaximoPorAnoBrasilOrdenado.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);
        j1.setCombinerClass(ReduceEtapaA.class);

        j1.setMapOutputKeyClass(IntWritable.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(IntWritable.class);
        j1.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        j1.waitForCompletion(true);

        //configuração do job2
        Job j2 = new Job(c, "ordenacaoDecrescente");

        j2.setJarByClass(Q8MaximoPorAnoBrasilOrdenado.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);

        j2.setMapOutputKeyClass(ValorDecrescenteWritable.class);
        j2.setMapOutputValueClass(IntWritable.class);
        j2.setOutputKeyClass(IntWritable.class);
        j2.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        System.exit(j2.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            //retira o cabeçalho
            if (linha.startsWith("country_or_area")) { return; }

            String[] col = linha.split(";");

            //trata dados faltantes: linha incompleta, ano ou preço vazio
            if (col.length < 10 || col[1].isEmpty() || col[5].isEmpty()) { return; }

            if (col[0].equals("Brazil")) {
                int ano = Integer.parseInt(col[1]);
                long preco = Long.parseLong(col[5]);

                con.write(new IntWritable(ano), new LongWritable(preco));
                //(2016, 6088)
            }
        }
    }

    // usado como combiner e reduce do job 1: maior valor de cada ano
    public static class ReduceEtapaA extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
        public void reduce(IntWritable key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            //(2016, [6088, 3958, ...])
            long maximo = Long.MIN_VALUE;
            for (LongWritable v : values) {
                if (v.get() > maximo) {
                    maximo = v.get();
                }
            }
            con.write(key, new LongWritable(maximo));
        }
    }

    public static class MapEtapaB extends Mapper<LongWritable, Text, ValorDecrescenteWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            //linha = 2016	185235399101
            String[] valor = linha.split("\t");
            // valor = [2016, 185235399101]
            int ano = Integer.parseInt(valor[0]);
            long maximo = Long.parseLong(valor[1]);

            //o valor vira a chave, para o sort/shuffle ordenar por ele (do maior para o menor)
            con.write(new ValorDecrescenteWritable(maximo), new IntWritable(ano));
        }
    }

    public static class ReduceEtapaB extends Reducer<ValorDecrescenteWritable, IntWritable, IntWritable, LongWritable> {
        public void reduce(ValorDecrescenteWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            //as chaves já chegam ordenadas do maior para o menor valor
            for (IntWritable ano : values) {
                con.write(ano, new LongWritable(key.getValor()));
            }
        }
    }
}
