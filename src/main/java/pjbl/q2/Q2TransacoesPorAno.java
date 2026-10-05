package pjbl.q2;

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

// Questão 2: número de transações por ano.
public class Q2TransacoesPorAno {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/q2");

        // criacao do job e seu nome
        Job j = new Job(c, "transacoesPorAno");

        //1. registro das classes
        j.setJarByClass(Q2TransacoesPorAno.class);
        j.setMapperClass(MapForAno.class);
        j.setReducerClass(ReduceForAno.class);
        j.setCombinerClass(ReduceForAno.class);

        //2. Definição dos tipos de saída
        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(IntWritable.class);
        j.setOutputValueClass(IntWritable.class);

        //3. Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4. Lança o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAno extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            //Afghanistan;2016;010410;Sheep, live;Export;6088;2339;Number of items;51;01_live_animals

            //retira o cabeçalho
            if (linha.startsWith("country_or_area")) { return; }

            String[] col = linha.split(";");
            //col = ["Afghanistan", "2016", "010410", "Sheep, live", "Export", "6088", "2339", "Number of items", "51", "01_live_animals"]

            //trata dados faltantes: linha incompleta ou ano vazio
            if (col.length < 10 || col[1].isEmpty()) { return; }

            //descarta as linhas TOTAL (ALL COMMODITIES): são a soma de todas as transações
            //do país no ano, não uma transação; mantê-las contaria os valores em dobro
            if (col[2].equals("TOTAL")) { return; }

            int ano = Integer.parseInt(col[1]);
            con.write(new IntWritable(ano), new IntWritable(1));
        }
    }

    // usado como combiner e como reduce: soma as ocorrências de cada chave
    public static class ReduceForAno extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }
            con.write(key, new IntWritable(soma));
        }
    }
}
