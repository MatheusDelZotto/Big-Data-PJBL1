package pjbl.q1;

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

import java.io.IOException;

// Questão 1: número de transações envolvendo o Brasil.
public class Q1TransacoesBrasil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/q1");

        // criacao do job e seu nome
        Job j = new Job(c, "transacoesBrasil");

        //1. registro das classes
        j.setJarByClass(Q1TransacoesBrasil.class);
        j.setMapperClass(MapForBrasil.class);
        j.setReducerClass(ReduceForBrasil.class);
        j.setCombinerClass(ReduceForBrasil.class);

        //2. Definição dos tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //3. Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4. Lança o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForBrasil extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            //Afghanistan;2016;010410;Sheep, live;Export;6088;2339;Number of items;51;01_live_animals

            //retira o cabeçalho
            if (linha.startsWith("country_or_area")) { return; }

            String[] col = linha.split(";");
            //col = ["Afghanistan", "2016", "010410", "Sheep, live", "Export", "6088", "2339", "Number of items", "51", "01_live_animals"]

            //trata dados faltantes: linha incompleta ou país vazio
            if (col.length < 10 || col[0].isEmpty()) { return; }

            if (col[0].equals("Brazil")) {
                con.write(new Text("Brazil"), new IntWritable(1));
            }
        }
    }

    // usado como combiner e como reduce: soma as ocorrências de cada chave
    public static class ReduceForBrasil extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }
            con.write(key, new IntWritable(soma));
        }
    }
}
