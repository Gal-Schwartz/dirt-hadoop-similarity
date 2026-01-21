package com.dirt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
// Explicitly using Hadoop Text
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.*;

public class DirtDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DirtDriver(), args));
    }

    // --- DATA STRUCTURES ---

    public static class Token {
        public String word, pos, dep;
        public int head;

        public Token(String w, String p, String d, int h) {
            this.word = w; this.pos = p; this.dep = d; this.head = h;
        }
        public boolean isNoun() { return pos != null && pos.startsWith("N"); }
        public boolean isVerb() { return pos != null && pos.startsWith("V"); }
        public boolean isPrep() { return "IN".equals(pos) || "TO".equals(pos); }
    }

    public static class PathSlotKey implements WritableComparable<PathSlotKey> {
        public Text path = new Text();
        public Text slot = new Text();
        public IntWritable type = new IntWritable(); // 0 = Margin, 1 = Triple

        public PathSlotKey() {}
        public PathSlotKey(String p, String s, int t) {
            path.set(p); slot.set(s); type.set(t);
        }

        @Override public void write(DataOutput out) throws IOException {
            path.write(out); slot.write(out); type.write(out);
        }
        @Override public void readFields(DataInput in) throws IOException {
            path.readFields(in); slot.readFields(in); type.readFields(in);
        }
        @Override public int compareTo(PathSlotKey o) {
            int cmp = path.compareTo(o.path);
            if (cmp != 0) return cmp;
            cmp = slot.compareTo(o.slot);
            if (cmp != 0) return cmp;
            return type.compareTo(o.type);
        }
    }

    public static class PathSlotGroupingComparator extends WritableComparator {
        protected PathSlotGroupingComparator() { super(PathSlotKey.class, true); }
        @Override public int compare(WritableComparable a, WritableComparable b) {
            PathSlotKey k1 = (PathSlotKey) a;
            PathSlotKey k2 = (PathSlotKey) b;
            int cmp = k1.path.compareTo(k2.path);
            if (cmp != 0) return cmp;
            return k1.slot.compareTo(k2.slot);
        }
    }

    public static class PorterStemmer {
        public String stem(String s) {
            if (s == null) return "";
            s = s.toLowerCase();
            if (s.length() <= 2) return s;
            if (s.endsWith("sses")) return s.substring(0, s.length() - 2);
            if (s.endsWith("ies")) return s.substring(0, s.length() - 2) + "i";
            if (s.endsWith("s") && !s.endsWith("ss")) return s.substring(0, s.length() - 1);
            if (s.endsWith("ing")) return s.substring(0, s.length() - 3);
            if (s.endsWith("ed")) return s.substring(0, s.length() - 2);
            return s;
        }
    }

    public static class PathExtractor {
        private final PorterStemmer stemmer = new PorterStemmer();
        private static final Set<String> AUX = new HashSet<>(Arrays.asList(
            "be", "am", "is", "are", "was", "were", "been", "being", 
            "do", "does", "did", "have", "has", "had", "will", "would", 
            "shall", "should", "can", "could", "may", "might", "must"
        ));

        public List<String> extractPaths(List<Token> tokens) {
            List<String> results = new ArrayList<>();
            List<Integer> nouns = new ArrayList<>();
            for (int i = 0; i < tokens.size(); i++) {
                if (tokens.get(i).isNoun()) nouns.add(i);
            }

            for (int i = 0; i < nouns.size(); i++) {
                for (int j = i + 1; j < nouns.size(); j++) {
                    int src = nouns.get(i);
                    int dst = nouns.get(j);
                    List<Integer> path = getShortestPath(tokens, src, dst);
                    if (path != null && isValid(tokens, path)) {
                        String pStr = buildPath(tokens, path);
                        String x = stemmer.stem(tokens.get(src).word);
                        String y = stemmer.stem(tokens.get(dst).word);
                        results.add(pStr + "\t" + x + "\t" + y);
                    }
                }
            }
            return results;
        }

        private boolean isValid(List<Token> toks, List<Integer> path) {
            for (int idx : path) {
                Token t = toks.get(idx);
                if (t.isVerb() && !AUX.contains(t.word.toLowerCase())) return true;
            }
            return false;
        }

        private String buildPath(List<Token> toks, List<Integer> path) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < path.size(); i++) {
                int curr = path.get(i);
                Token t = toks.get(curr);
                if (i == 0 || i == path.size() - 1) sb.append("N");
                else if (t.isVerb()) sb.append("V:").append(stemmer.stem(t.word));
                else if (t.isPrep()) sb.append("P:").append(t.word.toLowerCase());
                else sb.append("W:").append(t.word.toLowerCase());

                if (i < path.size() - 1) {
                    int next = path.get(i + 1);
                    Token tNext = toks.get(next);
                    String rel = (t.head - 1 == next) ? "<" + t.dep : ">" + tNext.dep;
                    sb.append(":").append(rel).append(":");
                }
            }
            return sb.toString();
        }

        private List<Integer> getShortestPath(List<Token> tokens, int src, int dst) {
            int n = tokens.size();
            List<Integer>[] adj = new List[n];
            for (int i = 0; i < n; i++) adj[i] = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int h = tokens.get(i).head - 1;
                if (h >= 0 && h < n) { adj[i].add(h); adj[h].add(i); }
            }
            int[] prev = new int[n]; Arrays.fill(prev, -1);
            Queue<Integer> q = new LinkedList<>(); q.add(src); prev[src] = src;
            while (!q.isEmpty()) {
                int u = q.poll(); if (u == dst) break;
                for (int v : adj[u]) { if (prev[v] == -1) { prev[v] = u; q.add(v); } }
            }
            if (prev[dst] == -1) return null;
            List<Integer> path = new ArrayList<>(); int curr = dst;
            while (curr != src) { path.add(curr); curr = prev[curr]; }
            path.add(src); Collections.reverse(path);
            return path;
        }
    }

    // --- JOB 1: Extraction ---
    public static class Job1_Extraction {
        public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
            private final PathExtractor extractor = new PathExtractor();
            private final LongWritable outVal = new LongWritable();
            private final Text outKey = new Text();

            @Override protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] parts = line.split("\t");
                
                if (parts.length < 3) return;

                String textContent = parts[1];
                long count = 1;
                try {
                    count = Long.parseLong(parts[2]);
                } catch (NumberFormatException e) { 
                    count = 1; 
                }

                List<Token> tokens = parse(textContent);
                if (tokens == null || tokens.isEmpty()) return;
                
                for (String ex : extractor.extractPaths(tokens)) {
                    String[] f = ex.split("\t", -1);
                    if (f.length < 3) continue;

                    emit(context, "TRIPLE\t" + f[0] + "\tX\t" + f[1], count);
                    emit(context, "TRIPLE\t" + f[0] + "\tY\t" + f[2], count);
                    emit(context, "SW_MARGIN\tX\t" + f[1], count);
                    emit(context, "SW_MARGIN\tY\t" + f[2], count);
                    emit(context, "PS_MARGIN\t" + f[0] + "\tX", count);
                    emit(context, "PS_MARGIN\t" + f[0] + "\tY", count);
                    emit(context, "GLOBAL", count * 2);
                }
            }
            
            private void emit(Context ctx, String k, long v) throws IOException, InterruptedException {
                outKey.set(k); outVal.set(v); ctx.write(outKey, outVal);
            }
            
            private List<Token> parse(String ngram) {
                List<Token> tokens = new ArrayList<>();
                StringTokenizer st = new StringTokenizer(ngram, " ");
                while (st.hasMoreTokens()) {
                    String tokenStr = st.nextToken();
                    int lastSlash = tokenStr.lastIndexOf('/');
                    if (lastSlash == -1) continue;
                    int secondLastSlash = tokenStr.lastIndexOf('/', lastSlash - 1);
                    if (secondLastSlash == -1) continue;
                    int thirdLastSlash = tokenStr.lastIndexOf('/', secondLastSlash - 1);
                    if (thirdLastSlash == -1) continue;
                    try {
                        String headStr = tokenStr.substring(lastSlash + 1);
                        String dep = tokenStr.substring(secondLastSlash + 1, lastSlash);
                        String pos = tokenStr.substring(thirdLastSlash + 1, secondLastSlash);
                        String word = tokenStr.substring(0, thirdLastSlash);
                        tokens.add(new Token(word, pos, dep, Integer.parseInt(headStr)));
                    } catch (Exception e) {
                        continue;
                    }
                }
                return tokens;
            }
        }
        
        public static class Combine extends Reducer<Text, LongWritable, Text, LongWritable> {
            @Override protected void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
                long sum = 0; for (LongWritable v : values) sum += v.get();
                ctx.write(key, new LongWritable(sum));
            }
        }
        public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
            private MultipleOutputs<Text, LongWritable> mos;
            @Override protected void setup(Context context) { mos = new MultipleOutputs<>(context); }
            @Override protected void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
                long sum = 0; for (LongWritable v : values) sum += v.get();
                String k = key.toString();
                if (k.startsWith("TRIPLE")) mos.write("triples", key, new LongWritable(sum));
                else if (k.startsWith("PS_MARGIN")) mos.write("pathmargins", key, new LongWritable(sum));
                else if (k.startsWith("SW_MARGIN")) mos.write("wordmargins", key, new LongWritable(sum));
                else if (k.startsWith("GLOBAL")) mos.write("global", key, new LongWritable(sum));
            }
            @Override protected void cleanup(Context ctx) throws IOException, InterruptedException { mos.close(); }
        }
    }

    // --- JOB 2: MI Calculation ---
    public static class Job2_MI {
        public static class Map extends Mapper<LongWritable, Text, PathSlotKey, Text> {
            private java.util.Map<String, Long> wordMargins = new HashMap<>();
            @Override protected void setup(Context context) throws IOException {
                URI[] files = context.getCacheFiles();
                System.err.println("--- SETUP START ---"); 
                if (files != null) {
                    for (URI uri : files) {
                        try (BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath())))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                String[] p = line.split("\t");
                                // We expect: SW_MARGIN  type  word  count
                                if (p.length >= 4) {
                                    wordMargins.put(p[1] + "|" + p[2], Long.parseLong(p[3]));
                                }
                            }
                        } catch (Exception e) {
                            // If a folder or irrelevant file is passed, just ignore
                        }
                    }
                }
                else{
                    System.err.println("WARNING: No cache files found!");

                }
                System.err.println("Total WordMargins loaded: " + wordMargins.size());
            }
            @Override protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] p = line.split("\t");
                
                if (line.startsWith("PS_MARGIN") && p.length >= 4) {
                    context.write(new PathSlotKey(p[1], p[2], 0), new Text(p[3]));
                } else if (line.startsWith("TRIPLE") && p.length >= 5) {
                    Long sw = wordMargins.get(p[2] + "|" + p[3]); // Look up X|word
                    if (sw != null) {
                        context.write(new PathSlotKey(p[1], p[2], 1), new Text(p[3] + "\t" + p[4] + "\t" + sw));
                    }
                }
            }
        }
        public static class Reduce extends Reducer<PathSlotKey, Text, Text, Text> {
            private long N = 1;
            @Override protected void setup(Context context) { N = context.getConfiguration().getLong("GLOBAL_N", 1); }
            @Override protected void reduce(PathSlotKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                Iterator<Text> it = values.iterator();
                if (!it.hasNext()) return;
                
                long psCount = Long.parseLong(it.next().toString());
                
                while (it.hasNext()) {
                    String val = it.next().toString();
                    String[] data = val.split("\t"); 
                    
                    if (data.length < 3) continue;

                    double countTriple = Double.parseDouble(data[1]);
                    double countWord = Double.parseDouble(data[2]);
                    
                    double mi = Math.log((countTriple * N) / (psCount * countWord));
                    
                    if (mi > 0.001) { 
                         context.write(key.path, new Text(key.slot.toString() + "\t" + data[0] + "\t" + mi));
                    }
                }
            }
        }
    }

    // --- JOB 2.5: SumMI ---
    public static class Job25_SumMI {
        public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
            @Override protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().split("\t");
                if (parts.length < 4) return;
                context.write(new Text(parts[0] + "\t" + parts[1]), new DoubleWritable(Double.parseDouble(parts[3])));
            }
        }
        public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
            @Override protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
                double sum = 0; for (DoubleWritable v : values) sum += v.get();
                context.write(key, new DoubleWritable(sum));
            }
        }
    }

    // --- JOB 3: Overlap ---
    public static class Job3_Overlap {
        public static class Map extends Mapper<LongWritable, Text, Text, Text> {
            private java.util.Map<String, List<String>> neighbors = new HashMap<>();

            @Override protected void setup(Context context) throws IOException {
                URI[] files = context.getCacheFiles();
                if (files != null) {
                    for (URI uri : files) {
                        File f = new File(new Path(uri).getName());
                        if (f.getName().contains("preds")) {
                            loadTestSet(f);
                        }
                    }
                }
            }
            
            private void loadTestSet(File file) throws IOException {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] p = line.split("\t");
                        if (p.length >= 2) {
                            neighbors.computeIfAbsent(p[0], k -> new ArrayList<>()).add(p[1]);
                            neighbors.computeIfAbsent(p[1], k -> new ArrayList<>()).add(p[0]);
                        }
                    }
                }
            }

            @Override protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().split("\t");
                if (parts.length < 4) return;
                String path = parts[0];
                if (neighbors.containsKey(path)) {
                    String featureVal = parts[1] + "\t" + parts[2] + "\t" + parts[3];
                    for (String other : neighbors.get(path)) {
                        String p1 = (path.compareTo(other) <= 0) ? path : other;
                        String p2 = (path.compareTo(other) <= 0) ? other : path;
                        context.write(new Text(p1 + "\t" + p2), new Text(path + "\t" + featureVal));
                    }
                }
            }
        }
        public static class Reduce extends Reducer<Text, Text, Text, Text> {
            @Override protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String[] keyPaths = key.toString().split("\t");
                String p1Key = keyPaths[0]; String p2Key = keyPaths[1];
                java.util.Map<String, Double> v1 = new HashMap<>();
                java.util.Map<String, Double> v2 = new HashMap<>();
                for (Text val : values) {
                    String[] parts = val.toString().split("\t");
                    String recordPath = parts[0];
                    String feature = parts[1] + ":" + parts[2];
                    double mi = Double.parseDouble(parts[3]);
                    if (recordPath.equals(p1Key)) v1.put(feature, mi);
                    else if (recordPath.equals(p2Key)) v2.put(feature, mi);
                }
                double numX = 0.0, numY = 0.0;
                for (String feat : v1.keySet()) {
                    if (v2.containsKey(feat)) {
                        double sum = v1.get(feat) + v2.get(feat);
                        if (feat.endsWith(":X")) numX += sum;
                        else if (feat.endsWith(":Y")) numY += sum;
                    }
                }
                context.write(key, new Text(numX + "\t" + numY));
            }
        }
    }

    // --- JOB 4: Final Similarity ---
    public static class Job4_FinalSim {
        public static class Map extends Mapper<LongWritable, Text, Text, Text> {
            @Override protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().split("\t");
                if (parts.length < 4) return;
                context.write(new Text(parts[0] + "\t" + parts[1]), new Text(parts[2] + "\t" + parts[3]));
            }
        }
        public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
            private java.util.Map<String, Double> sumMIs = new HashMap<>();
            @Override protected void setup(Context context) throws IOException {
                URI[] files = context.getCacheFiles();
                if (files != null) {
                    for (URI uri : files) {
                        try (BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath())))) {
                             String line;
                             while ((line = br.readLine()) != null) {
                                 String[] p = line.split("\t");
                                 if (p.length >= 2) sumMIs.put(p[0], Double.parseDouble(p[1]));
                             }
                        } catch(Exception e) {}
                    }
                }
            }
           
            @Override protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                String[] paths = key.toString().split("\t");
                double numX = 0, numY = 0;
                for (Text val : values) {
                    String[] nums = val.toString().split("\t");
                    numX += Double.parseDouble(nums[0]);
                    numY += Double.parseDouble(nums[1]);
                }
                double s1X = sumMIs.getOrDefault(paths[0] + "\tX", 0.0);
                double s1Y = sumMIs.getOrDefault(paths[0] + "\tY", 0.0);
                double s2X = sumMIs.getOrDefault(paths[1] + "\tX", 0.0);
                double s2Y = sumMIs.getOrDefault(paths[1] + "\tY", 0.0);
                
                double simX = (s1X + s2X > 0) ? numX / (s1X + s2X) : 0;
                double simY = (s1Y + s2Y > 0) ? numY / (s1Y + s2Y) : 0;
                
                context.write(key, new DoubleWritable(Math.sqrt(simX * simY)));
            }
        }
    }

    // --- MAIN DRIVER ---
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        if (args.length < 1) {
            System.err.println("Usage: DirtDriver <input_path>");
            return 1;
        }
        String input = args[0];
        
        String outputBase = "s3://lexico-syntactic-similarities/output";
        String testSetBase = "s3://lexico-syntactic-similarities/TestSet"; 

        String out1 = outputBase + "/step1";
        String out2 = outputBase + "/step2";
        String out25 = outputBase + "/step2_5";
        String out3 = outputBase + "/step3";
        String out4 = outputBase + "/final";

        // JOB 1
        Job j1 = Job.getInstance(conf, "DIRT_1_Extraction");
        j1.setJarByClass(DirtDriver.class);
        j1.setMapperClass(Job1_Extraction.Map.class);
        j1.setCombinerClass(Job1_Extraction.Combine.class);
        j1.setReducerClass(Job1_Extraction.Reduce.class);
        j1.setOutputKeyClass(Text.class); j1.setOutputValueClass(LongWritable.class);
        MultipleOutputs.addNamedOutput(j1, "triples", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(j1, "pathmargins", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(j1, "wordmargins", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(j1, "global", TextOutputFormat.class, Text.class, LongWritable.class);
        FileInputFormat.addInputPath(j1, new Path(input));
        FileOutputFormat.setOutputPath(j1, new Path(out1));
        if (!j1.waitForCompletion(true)) return 1;

        // FIX: Look for specific file prefixes in Step 1 output for Global N
        // FIXED: Wrapped 'out1' string in 'new Path()'
        long globalN = readTotalN(conf, new Path(out1), "global");
        conf.setLong("GLOBAL_N", globalN);

        // JOB 2
        Job j2 = Job.getInstance(conf, "DIRT_2_MI");
        j2.setJarByClass(DirtDriver.class);
        addCacheFilesWithPrefix(j2, conf, new Path(out1), "wordmargins");
        
        j2.setMapperClass(Job2_MI.Map.class);
        j2.setGroupingComparatorClass(PathSlotGroupingComparator.class);
        j2.setReducerClass(Job2_MI.Reduce.class);
        j2.setMapOutputKeyClass(PathSlotKey.class); j2.setMapOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(j2, new Path(out1 + "/triples*"));
        FileInputFormat.addInputPath(j2, new Path(out1 + "/pathmargins*"));
        
        FileOutputFormat.setOutputPath(j2, new Path(out2));
        if (!j2.waitForCompletion(true)) return 1;

        // JOB 2.5
        Job j25 = Job.getInstance(conf, "DIRT_2.5_SumMI");
        j25.setJarByClass(DirtDriver.class);
        j25.setMapperClass(Job25_SumMI.Map.class);
        j25.setCombinerClass(Job25_SumMI.Reduce.class);
        j25.setReducerClass(Job25_SumMI.Reduce.class);
        j25.setOutputKeyClass(Text.class); j25.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j25, new Path(out2));
        FileOutputFormat.setOutputPath(j25, new Path(out25));
        if (!j25.waitForCompletion(true)) return 1;

        // JOB 3
        Job j3 = Job.getInstance(conf, "DIRT_3_Overlap");
        j3.setJarByClass(DirtDriver.class);
        j3.addCacheFile(new URI(testSetBase + "/positive-preds.txt"));
        j3.addCacheFile(new URI(testSetBase + "/negative-preds.txt"));
        j3.setMapperClass(Job3_Overlap.Map.class);
        j3.setReducerClass(Job3_Overlap.Reduce.class);
        j3.setOutputKeyClass(Text.class); j3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j3, new Path(out2));
        FileOutputFormat.setOutputPath(j3, new Path(out3));
        if (!j3.waitForCompletion(true)) return 1;

        // JOB 4
        Job j4 = Job.getInstance(conf, "DIRT_4_FinalSim");
        j4.setJarByClass(DirtDriver.class);
        addAllPartsToCache(j4, conf, new Path(out25));
        j4.setMapperClass(Job4_FinalSim.Map.class);
        j4.setReducerClass(Job4_FinalSim.Reduce.class);
        j4.setOutputKeyClass(Text.class); j4.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j4, new Path(out3));
        FileOutputFormat.setOutputPath(j4, new Path(out4));
        
        return j4.waitForCompletion(true) ? 0 : 1;
    }

    // --- HELPER METHODS ---

    private void addCacheFilesWithPrefix(Job job, Configuration conf, Path parentDir, String prefix) throws IOException {
        FileSystem fs = parentDir.getFileSystem(conf);
        if (fs.exists(parentDir)) {
            FileStatus[] stats = fs.listStatus(parentDir);
            for (FileStatus stat : stats) {
                if (stat.getPath().getName().startsWith(prefix)) {
                    job.addCacheFile(stat.getPath().toUri());
                }
            }
        }
    }

    private void addAllPartsToCache(Job job, Configuration conf, Path dir) throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        if (fs.exists(dir)) {
            FileStatus[] stats = fs.listStatus(dir);
            for (FileStatus st : stats) {
                if (st.getPath().getName().startsWith("part-")) {
                    job.addCacheFile(st.getPath().toUri());
                }
            }
        }
    }

    private long readTotalN(Configuration conf, Path parentDir, String prefix) throws IOException {
        long sum = 0;
        FileSystem fs = parentDir.getFileSystem(conf);
        if (fs.exists(parentDir)) {
            FileStatus[] stats = fs.listStatus(parentDir);
            for (FileStatus stat : stats) {
                if (stat.getPath().getName().startsWith(prefix)) {
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stat.getPath())))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] partsStr = line.split("\t");
                            if (partsStr.length >= 2) sum += Long.parseLong(partsStr[1]);
                        }
                    }
                }
            }
        }
        return sum == 0 ? 1 : sum;
    }
}