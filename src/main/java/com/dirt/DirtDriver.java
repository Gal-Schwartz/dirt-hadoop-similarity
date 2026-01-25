package com.dirt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
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
            this.word = w;
            this.pos = p;
            this.dep = d;
            this.head = h;
        }

        public boolean isNoun() {
            return pos != null && pos.startsWith("N");
        }

        public boolean isVerb() {
            return pos != null && pos.startsWith("V");
        }

        public boolean isPrep() {
            return "IN".equals(pos) || "TO".equals(pos);
        }
    }

    public static class PathSlotKey implements WritableComparable<PathSlotKey> {
        public Text path = new Text();
        public Text slot = new Text();
        public IntWritable type = new IntWritable(); // 0 = Margin, 1 = Triple

        public PathSlotKey() {
        }

        public PathSlotKey(String p, String s, int t) {
            path.set(p);
            slot.set(s);
            type.set(t);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            path.write(out);
            slot.write(out);
            type.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            path.readFields(in);
            slot.readFields(in);
            type.readFields(in);
        }

        @Override
        public int compareTo(PathSlotKey o) {
            int cmp = path.compareTo(o.path);
            if (cmp != 0)
                return cmp;
            cmp = slot.compareTo(o.slot);
            if (cmp != 0)
                return cmp;
            return type.compareTo(o.type);
        }
    }

    public static class PathSlotGroupingComparator extends WritableComparator {
        protected PathSlotGroupingComparator() {
            super(PathSlotKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PathSlotKey k1 = (PathSlotKey) a;
            PathSlotKey k2 = (PathSlotKey) b;
            int cmp = k1.path.compareTo(k2.path);
            if (cmp != 0)
                return cmp;
            return k1.slot.compareTo(k2.slot);
        }
    }

    public static class PorterStemmer {
        private char[] b;
        private int i, i_end, j, k;
        private static final int INC = 50;

        public PorterStemmer() {
            b = new char[INC];
            i = 0;
            i_end = 0;
        }

        public String stem(String s) {
            if (s == null || s.length() == 0) return "";
            
            // Reset buffer
            i = 0;
            i_end = 0;
            
            // Add string to buffer
            char[] chars = s.toCharArray();
            for (char c : chars) {
                add(c);
            }
            
            stem();
            return toString();
        }

        public void add(char ch) {
            if (i == b.length) {
                char[] new_b = new char[i + INC];
                for (int c = 0; c < i; c++) new_b[c] = b[c];
                b = new_b;
            }
            b[i++] = ch;
        }

        public String toString() {
            return new String(b, 0, i_end);
        }

        private final boolean cons(int i) {
            switch (b[i]) {
                case 'a': case 'e': case 'i': case 'o': case 'u': return false;
                case 'y': return (i == 0) ? true : !cons(i - 1);
                default: return true;
            }
        }

        private final int m() {
            int n = 0;
            int i = 0;
            while (true) {
                if (i > j) return n;
                if (!cons(i)) break;
                i++;
            }
            i++;
            while (true) {
                while (true) {
                    if (i > j) return n;
                    if (cons(i)) break;
                    i++;
                }
                i++;
                n++;
                while (true) {
                    if (i > j) return n;
                    if (!cons(i)) break;
                    i++;
                }
                i++;
            }
        }

        private final boolean vowelinstem() {
            int i;
            for (i = 0; i <= j; i++)
                if (!cons(i)) return true;
            return false;
        }

        private final boolean doublec(int j) {
            if (j < 1) return false;
            if (b[j] != b[j - 1]) return false;
            return cons(j);
        }

        private final boolean cvc(int i) {
            if (i < 2 || !cons(i) || cons(i - 1) || !cons(i - 2)) return false;
            int ch = b[i];
            if (ch == 'w' || ch == 'x' || ch == 'y') return false;
            return true;
        }

        private final boolean ends(String s) {
            int l = s.length();
            int o = k - l + 1;
            if (o < 0) return false;
            for (int i = 0; i < l; i++)
                if (b[o + i] != s.charAt(i)) return false;
            j = k - l;
            return true;
        }

        private final void setto(String s) {
            int l = s.length();
            int o = j + 1;
            for (int i = 0; i < l; i++)
                b[o + i] = s.charAt(i);
            k = j + l;
        }

        private final void r(String s) {
            if (m() > 0) setto(s);
        }

        private final void step1() {
            if (b[k] == 's') {
                if (ends("sses")) k -= 2;
                else if (ends("ies")) setto("i");
                else if (b[k - 1] != 's') k--;
            }
            if (ends("eed")) {
                if (m() > 0) k--;
            } else if ((ends("ed") || ends("ing")) && vowelinstem()) {
                k = j;
                if (ends("at")) setto("ate");
                else if (ends("bl")) setto("ble");
                else if (ends("iz")) setto("ize");
                else if (doublec(k)) {
                    k--;
                    int ch = b[k];
                    if (ch == 'l' || ch == 's' || ch == 'z') k++;
                } else if (m() == 1 && cvc(k)) setto("e");
            }
        }

        private final void step2() {
            if (ends("y") && vowelinstem()) b[k] = 'i';
        }

        private final void step3() {
            if (k == 0) return;
            switch (b[k - 1]) {
                case 'a':
                    if (ends("ational")) { r("ate"); break; }
                    if (ends("tional")) { r("tion"); break; }
                    break;
                case 'c':
                    if (ends("enci")) { r("ence"); break; }
                    if (ends("anci")) { r("ance"); break; }
                    break;
                case 'e':
                    if (ends("izer")) { r("ize"); break; }
                    break;
                case 'l':
                    if (ends("bli")) { r("ble"); break; }
                    if (ends("alli")) { r("al"); break; }
                    if (ends("entli")) { r("ent"); break; }
                    if (ends("eli")) { r("e"); break; }
                    if (ends("ousli")) { r("ous"); break; }
                    break;
                case 'o':
                    if (ends("ization")) { r("ize"); break; }
                    if (ends("ation")) { r("ate"); break; }
                    if (ends("ator")) { r("ate"); break; }
                    break;
                case 's':
                    if (ends("alism")) { r("al"); break; }
                    if (ends("iveness")) { r("ive"); break; }
                    if (ends("fulness")) { r("ful"); break; }
                    if (ends("ousness")) { r("ous"); break; }
                    break;
                case 't':
                    if (ends("aliti")) { r("al"); break; }
                    if (ends("iviti")) { r("ive"); break; }
                    if (ends("biliti")) { r("ble"); break; }
                    break;
                case 'g':
                    if (ends("logi")) { r("log"); break; }
            }
        }

        private final void step4() {
            switch (b[k]) {
                case 'e':
                    if (ends("icate")) { r("ic"); break; }
                    if (ends("ative")) { r(""); break; }
                    if (ends("alize")) { r("al"); break; }
                    break;
                case 'i':
                    if (ends("iciti")) { r("ic"); break; }
                    break;
                case 'l':
                    if (ends("ical")) { r("ic"); break; }
                    if (ends("ful")) { r(""); break; }
                    break;
                case 's':
                    if (ends("ness")) { r(""); break; }
                    break;
            }
        }

        private final void step5() {
            if (k == 0) return;
            switch (b[k - 1]) {
                case 'a':
                    if (ends("al")) break; return;
                case 'c':
                    if (ends("ance")) break;
                    if (ends("ence")) break; return;
                case 'e':
                    if (ends("er")) break; return;
                case 'i':
                    if (ends("ic")) break; return;
                case 'l':
                    if (ends("able")) break;
                    if (ends("ible")) break; return;
                case 'n':
                    if (ends("ant")) break;
                    if (ends("ement")) break;
                    if (ends("ment")) break;
                    if (ends("ent")) break; return;
                case 'o':
                    if (ends("ion") && j >= 0 && (b[j] == 's' || b[j] == 't')) break;
                    if (ends("ou")) break; return;
                case 's':
                    if (ends("ism")) break; return;
                case 't':
                    if (ends("ate")) break;
                    if (ends("iti")) break; return;
                case 'u':
                    if (ends("ous")) break; return;
                case 'v':
                    if (ends("ive")) break; return;
                case 'z':
                    if (ends("ize")) break; return;
                default: return;
            }
            if (m() > 1) k = j;
        }

        private final void step6() {
            j = k;
            if (b[k] == 'e') {
                int a = m();
                if (a > 1 || a == 1 && !cvc(k - 1)) k--;
            }
            if (b[k] == 'l' && doublec(k) && m() > 1) k--;
        }

        public void stem() {
            k = i - 1;
            if (k > 1) {
                step1();
                step2();
                step3();
                step4();
                step5();
                step6();
            }
            i_end = k + 1;
            i = 0;
        }
    }

    public static class PathExtractor {
        private final PorterStemmer stemmer = new PorterStemmer();
        private static final Set<String> AUX = new HashSet<>(Arrays.asList(
                "be", "am", "is", "are", "was", "were", "been", "being",
                "do", "does", "did", "have", "has", "had", "will", "would",
                "shall", "should", "can", "could", "may", "might", "must"));

        public List<String> extractPaths(List<Token> tokens) {
            List<String> results = new ArrayList<>();
            List<Integer> nouns = new ArrayList<>();
            for (int i = 0; i < tokens.size(); i++) {
                if (tokens.get(i).isNoun())
                    nouns.add(i);
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
                if (t.isVerb() && !AUX.contains(t.word.toLowerCase()))
                    return true;
            }
            return false;
        }

        private String buildPath(List<Token> toks, List<Integer> path) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < path.size(); i++) {
                int curr = path.get(i);
                Token t = toks.get(curr);
                if (i == 0 || i == path.size() - 1)
                    sb.append("N");
                else if (t.isVerb())
                    sb.append("V:").append(stemmer.stem(t.word));
                else if (t.isPrep())
                    sb.append("P:").append(t.word.toLowerCase());
                else
                    sb.append("W:").append(t.word.toLowerCase());

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
            for (int i = 0; i < n; i++)
                adj[i] = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                int h = tokens.get(i).head - 1;
                if (h >= 0 && h < n) {
                    adj[i].add(h);
                    adj[h].add(i);
                }
            }
            int[] prev = new int[n];
            Arrays.fill(prev, -1);
            Queue<Integer> q = new LinkedList<>();
            q.add(src);
            prev[src] = src;
            while (!q.isEmpty()) {
                int u = q.poll();
                if (u == dst)
                    break;
                for (int v : adj[u]) {
                    if (prev[v] == -1) {
                        prev[v] = u;
                        q.add(v);
                    }
                }
            }
            if (prev[dst] == -1)
                return null;
            List<Integer> path = new ArrayList<>();
            int curr = dst;
            while (curr != src) {
                path.add(curr);
                curr = prev[curr];
            }
            path.add(src);
            Collections.reverse(path);
            return path;
        }
    }

    // --- JOB 1: Extraction ---
    public static class Job1_Extraction {
        public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
            private final PathExtractor extractor = new PathExtractor();
            private final LongWritable outVal = new LongWritable();
            private final Text outKey = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] parts = line.split("\t");

                if (parts.length < 3)
                    return;

                String textContent = parts[1];
                long count = 1;
                try {
                    count = Long.parseLong(parts[2]);
                } catch (NumberFormatException e) {
                    count = 1;
                }

                List<Token> tokens = parse(textContent);
                if (tokens == null || tokens.isEmpty())
                    return;

                for (String ex : extractor.extractPaths(tokens)) {
                    String[] f = ex.split("\t", -1);
                    if (f.length < 3)
                        continue;

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
                outKey.set(k);
                outVal.set(v);
                ctx.write(outKey, outVal);
            }

            private List<Token> parse(String ngram) {
                List<Token> tokens = new ArrayList<>();
                StringTokenizer st = new StringTokenizer(ngram, " ");
                while (st.hasMoreTokens()) {
                    String tokenStr = st.nextToken();
                    int lastSlash = tokenStr.lastIndexOf('/');
                    if (lastSlash == -1)
                        continue;
                    int secondLastSlash = tokenStr.lastIndexOf('/', lastSlash - 1);
                    if (secondLastSlash == -1)
                        continue;
                    int thirdLastSlash = tokenStr.lastIndexOf('/', secondLastSlash - 1);
                    if (thirdLastSlash == -1)
                        continue;
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
            @Override
            protected void reduce(Text key, Iterable<LongWritable> values, Context ctx)
                    throws IOException, InterruptedException {
                long sum = 0;
                for (LongWritable v : values)
                    sum += v.get();
                ctx.write(key, new LongWritable(sum));
            }
        }

        public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
            private MultipleOutputs<Text, LongWritable> mos;

            @Override
            protected void setup(Context context) {
                mos = new MultipleOutputs<>(context);
            }

            @Override
            protected void reduce(Text key, Iterable<LongWritable> values, Context ctx)
                    throws IOException, InterruptedException {
                long sum = 0;
                for (LongWritable v : values)
                    sum += v.get();
                String k = key.toString();
                if (k.startsWith("TRIPLE"))
                    mos.write("triples", key, new LongWritable(sum));
                else if (k.startsWith("PS_MARGIN"))
                    mos.write("pathmargins", key, new LongWritable(sum));
                else if (k.startsWith("SW_MARGIN"))
                    mos.write("wordmargins", key, new LongWritable(sum));
                else if (k.startsWith("GLOBAL"))
                    mos.write("global", key, new LongWritable(sum));
            }

            @Override
            protected void cleanup(Context ctx) throws IOException, InterruptedException {
                mos.close();
            }
        }
    }

    // --- JOB 2: MI Calculation ---
    public static class Job2_MI {
        public static class Map extends Mapper<LongWritable, Text, PathSlotKey, Text> {
            private java.util.Map<String, Long> wordMargins = new HashMap<>();

            @Override
            protected void setup(Context context) throws IOException {
                URI[] files = context.getCacheFiles();
                if (files != null) {
                    for (URI uri : files) {
                        try (BufferedReader br = new BufferedReader(
                                new FileReader(new File(new Path(uri).getName())))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                String[] p = line.split("\t");
                                if (p.length >= 4 && line.startsWith("SW_MARGIN")) {
                                    wordMargins.put(p[1] + "|" + p[2], Long.parseLong(p[3]));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("ERROR loading word margins from " + uri + ": " + e.getMessage());
                            throw new IOException("Failed to load critical cache file: " + uri, e);
                        }
                    }
                }
                if (wordMargins.isEmpty()) {
                    throw new IOException("No word margins loaded! Check cache files.");
                }
                System.err.println("Loaded " + wordMargins.size() + " word margin entries");
            }

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] p = line.split("\t");

                if (line.startsWith("PS_MARGIN") && p.length >= 4) {
                    context.write(new PathSlotKey(p[1], p[2], 0), new Text(p[3]));
                } else if (line.startsWith("TRIPLE") && p.length >= 5) {
                    Long sw = wordMargins.get(p[2] + "|" + p[3]);
                    if (sw != null) {
                        context.write(new PathSlotKey(p[1], p[2], 1), new Text(p[3] + "\t" + p[4] + "\t" + sw));
                    }
                }
            }
        }

        public static class Reduce extends Reducer<PathSlotKey, Text, Text, Text> {
            private long N = 1;

            @Override
            protected void setup(Context context) {
                N = context.getConfiguration().getLong("GLOBAL_N", 1);
                System.err.println("Using GLOBAL_N = " + N);
            }

            @Override
            protected void reduce(PathSlotKey key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                
                long psCount = 0;
                List<String> tripleRecords = new ArrayList<>();
                
                for (Text val : values) {
                    String valStr = val.toString();
                    
                    // Check if this is a margin (no tabs) or triple (has tabs)
                    if (valStr.contains("\t")) {
                        // This is a triple record
                        tripleRecords.add(valStr);
                    } else {
                        // This is a margin record
                        try {
                            psCount = Long.parseLong(valStr);
                        } catch (NumberFormatException e) {
                            System.err.println("ERROR parsing margin count: " + valStr);
                        }
                    }
                }
                
                if (psCount == 0) {
                    System.err.println("WARNING: No margin found for path=" + key.path + " slot=" + key.slot);
                    return;
                }
                
                for (String tripleData : tripleRecords) {
                    String[] data = tripleData.split("\t");
                    
                    if (data.length < 3)
                        continue;

                    try {
                        String word = data[0];
                        long tripleCount = Long.parseLong(data[1]);
                        long swCount = Long.parseLong(data[2]);

                        double numerator = (double) tripleCount * N;
                        double denominator = (double) psCount * swCount;

                        if (numerator > 0 && denominator > 0) {
                            double mi = Math.log(numerator / denominator);
                            if (mi > 0.001) {
                                context.write(key.path, new Text(key.slot.toString() + "\t" + word + "\t" + mi));
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("ERROR calculating MI for triple: " + tripleData + " - " + e.getMessage());
                    }
                }
            }
        }
    }

    // --- JOB 2.5: SumMI ---
    public static class Job25_SumMI {
        public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().split("\t");
                if (parts.length < 4)
                    return;
                context.write(new Text(parts[0] + "\t" + parts[1]), new DoubleWritable(Double.parseDouble(parts[3])));
            }
        }

        public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
            @Override
            protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                    throws IOException, InterruptedException {
                double sum = 0;
                for (DoubleWritable v : values)
                    sum += v.get();
                context.write(key, new DoubleWritable(sum));
            }
        }
    }

    // --- JOB 3: Overlap ---
    public static class Job3_Overlap {
        public static class Map extends Mapper<LongWritable, Text, Text, Text> {
            private java.util.Map<String, List<String>> neighbors = new HashMap<>();
            private final PorterStemmer stemmer = new PorterStemmer();
            private static final Set<String> AUX_WORDS = new HashSet<>(Arrays.asList(
                "be", "am", "is", "are", "was", "were", "been", "being",
                "do", "does", "did", "have", "has", "had", "will", "would",
                "shall", "should", "can", "could", "may", "might", "must"));

            @Override
            protected void setup(Context context) throws IOException {
                URI[] files = context.getCacheFiles();
                if (files != null) {
                    int filesLoaded = 0;
                    for (URI uri : files) {
                        try {
                            loadTestSet(new File(new Path(uri).getName()));
                            filesLoaded++;
                        } catch (Exception e) {
                            System.err.println("ERROR loading test set from " + uri + ": " + e.getMessage());
                            throw new IOException("Failed to load critical test set file: " + uri, e);
                        }
                    }
                    System.err.println("Loaded " + filesLoaded + " test set files");
                }
                if (neighbors.isEmpty()) {
                    throw new IOException("No test set pairs loaded! Check cache files.");
                }
                System.err.println("Total neighbor pairs: " + neighbors.size());
            }

            
            private String convertPhraseToPath(String phrase) {
                String inner = phrase.replaceAll("^X\\s+", "")
                                    .replaceAll("\\s+Y$", "")
                                    .trim();
                if (inner.isEmpty()) return null;

                String[] w = inner.split("\\s+");

                // Allow optional leading AUX
                int start = 0;
                if (w.length >= 2 && AUX_WORDS.contains(w[0].toLowerCase())) {
                    start = 1;
                }

                int len = w.length - start;
                if (len <= 0) return null;

                if (len >= 2 && w[w.length - 1].equalsIgnoreCase("by")) {
                    String verb = w[w.length - 2];
                    String vStem = stemmer.stem(verb);
                    return "N:<nsubjpass:V:" + vStem + ":>prep:P:by:>pobj:N";
                }

                // Active: "X VERB Y"
                if (len == 1) {
                    String vStem = stemmer.stem(w[start]);
                    return "N:<nsubj:V:" + vStem + ":>dobj:N";
                }

                // Active with prep: "X VERB PREP Y"
                if (len == 2) {
                    String vStem = stemmer.stem(w[start]);
                    String prep = w[start + 1].toLowerCase();
                    return "N:<nsubj:V:" + vStem + ":>prep:P:" + prep + ":>pobj:N";
                }
                if (len == 3) {
                    // Check if middle word is common particle
                    String middle = w[start + 1].toLowerCase();
                    if (isParticle(middle)) {
                        // Treat as "verb-particle prep" -> collapse to "verb prep"
                        String vStem = stemmer.stem(w[start] + middle); // composite stem
                        String prep = w[start + 2].toLowerCase();
                        return "N:<nsubj:V:" + vStem + ":>prep:P:" + prep + ":>pobj:N";
                    }
                }

                // Warn about unhandled patterns
                System.err.println("WARNING: Cannot convert phrase (len=" + len + "): " + phrase);
                return null;
            }
            
            private boolean isParticle(String word) {
                // Common verb particles
                return word.equals("up") || word.equals("down") || word.equals("out") || 
                       word.equals("in") || word.equals("off") || word.equals("on") ||
                       word.equals("over") || word.equals("through");
            }



            private void loadTestSet(File file) throws IOException {
                int pairCount = 0;
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] p = line.split("\t");
                        if (p.length >= 2) {
                            String path1 = convertPhraseToPath(p[0]);
                            String path2 = convertPhraseToPath(p[1]);

                            if (path1 != null && path2 != null) {
                                neighbors.computeIfAbsent(path1, k -> new ArrayList<>()).add(path2);
                                neighbors.computeIfAbsent(path2, k -> new ArrayList<>()).add(path1);
                                pairCount++;
                            } else {
                                if (path1 == null) System.err.println("  Could not convert: " + p[0]);
                                if (path2 == null) System.err.println("  Could not convert: " + p[1]);
                            }
                        }
                    }
                }
                System.err.println("Loaded " + pairCount + " pairs from " + file.getName());
            }

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().split("\t");
                if (parts.length < 4)
                    return;
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
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                String[] keyPaths = key.toString().split("\t");
                if (keyPaths.length < 2)
                    return;
                String p1Key = keyPaths[0];
                String p2Key = keyPaths[1];

                java.util.Map<String, Double> v1 = new HashMap<>();
                java.util.Map<String, Double> v2 = new HashMap<>();

                for (Text val : values) {
                    String[] parts = val.toString().split("\t");
                    if (parts.length < 4)
                        continue;
                    String recordPath = parts[0];
                    String feature = parts[1] + ":" + parts[2]; // Slot:Word
                    double mi = Double.parseDouble(parts[3]);

                    if (recordPath.equals(p1Key))
                        v1.put(feature, mi);
                    else if (recordPath.equals(p2Key))
                        v2.put(feature, mi);
                }

                double numX = 0.0, numY = 0.0;
                for (String feat : v1.keySet()) {
                    if (v2.containsKey(feat)) {
                        double sum = v1.get(feat) + v2.get(feat);
                        if (feat.startsWith("X:"))
                            numX += sum;
                        else if (feat.startsWith("Y:"))
                            numY += sum;
                    }
                }
                context.write(key, new Text(numX + "\t" + numY));
            }
        }
    }

    // --- JOB 4: Final Similarity ---
    public static class Job4_FinalSim {
        public static class Map extends Mapper<LongWritable, Text, Text, Text> {
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().split("\t");
                if (parts.length < 4)
                    return;
                context.write(new Text(parts[0] + "\t" + parts[1]), new Text(parts[2] + "\t" + parts[3]));
            }
        }

        public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
            private java.util.Map<String, Double> sumMIs = new HashMap<>();

            @Override
            protected void setup(Context context) throws IOException {
                URI[] files = context.getCacheFiles();
                if (files != null) {
                    int filesLoaded = 0;
                    for (URI uri : files) {
                        try (BufferedReader br = new BufferedReader(
                                new FileReader(new File(new Path(uri).getName())))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                String[] p = line.split("\t");
                                if (p.length >= 3) {
                                    sumMIs.put(p[0] + "\t" + p[1], Double.parseDouble(p[2]));
                                }
                            }
                            filesLoaded++;
                        } catch (Exception e) {
                            System.err.println("ERROR loading sumMI from " + uri + ": " + e.getMessage());
                            throw new IOException("Failed to load critical sumMI file: " + uri, e);
                        }
                    }
                    System.err.println("Loaded sumMI data from " + filesLoaded + " files");
                }
                if (sumMIs.isEmpty()) {
                    throw new IOException("No sumMI data loaded! Check cache files.");
                }
                System.err.println("Total sumMI entries: " + sumMIs.size());
            }

            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                String[] paths = key.toString().split("\t");
                if (paths.length < 2)
                    return;
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
        System.err.println("Starting Job 1: Extraction");
        Job j1 = Job.getInstance(conf, "DIRT_1_Extraction");
        j1.setJarByClass(DirtDriver.class);
        j1.setMapperClass(Job1_Extraction.Map.class);
        j1.setCombinerClass(Job1_Extraction.Combine.class);
        j1.setReducerClass(Job1_Extraction.Reduce.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);
        MultipleOutputs.addNamedOutput(j1, "triples", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(j1, "pathmargins", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(j1, "wordmargins", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(j1, "global", TextOutputFormat.class, Text.class, LongWritable.class);
        FileInputFormat.addInputPaths(j1, input);
        FileOutputFormat.setOutputPath(j1, new Path(out1));
        if (!j1.waitForCompletion(true))
            return 1;

        long globalN;
        try {
            globalN = readTotalN(conf, new Path(out1), "global");
            System.err.println("Read GLOBAL_N = " + globalN);
        } catch (Exception e) {
            System.err.println("ERROR reading global N: " + e.getMessage());
            return 1;
        }
        conf.setLong("GLOBAL_N", globalN);

        // JOB 2
        System.err.println("Starting Job 2: MI Calculation");
        Job j2 = Job.getInstance(conf, "DIRT_2_MI");
        j2.setJarByClass(DirtDriver.class);
        addCacheFilesWithPrefix(j2, conf, new Path(out1), "wordmargins");

        j2.setMapperClass(Job2_MI.Map.class);
        j2.setGroupingComparatorClass(PathSlotGroupingComparator.class);
        j2.setReducerClass(Job2_MI.Reduce.class);
        j2.setMapOutputKeyClass(PathSlotKey.class);
        j2.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j2, new Path(out1 + "/triples*"));
        FileInputFormat.addInputPath(j2, new Path(out1 + "/pathmargins*"));

        FileOutputFormat.setOutputPath(j2, new Path(out2));
        if (!j2.waitForCompletion(true))
            return 1;

        // JOB 2.5
        System.err.println("Starting Job 2.5: Sum MI");
        Job j25 = Job.getInstance(conf, "DIRT_2.5_SumMI");
        j25.setJarByClass(DirtDriver.class);
        j25.setMapperClass(Job25_SumMI.Map.class);
        j25.setCombinerClass(Job25_SumMI.Reduce.class);
        j25.setReducerClass(Job25_SumMI.Reduce.class);
        j25.setOutputKeyClass(Text.class);
        j25.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j25, new Path(out2));
        FileOutputFormat.setOutputPath(j25, new Path(out25));
        if (!j25.waitForCompletion(true))
            return 1;

        // JOB 3
        System.err.println("Starting Job 3: Overlap Calculation");
        Job j3 = Job.getInstance(conf, "DIRT_3_Overlap");
        j3.setJarByClass(DirtDriver.class);
        j3.addCacheFile(new URI(testSetBase + "/positive-preds.txt"));
        j3.addCacheFile(new URI(testSetBase + "/negative-preds.txt"));
        j3.setMapperClass(Job3_Overlap.Map.class);
        j3.setReducerClass(Job3_Overlap.Reduce.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j3, new Path(out2));
        FileOutputFormat.setOutputPath(j3, new Path(out3));
        if (!j3.waitForCompletion(true))
            return 1;

        // JOB 4
        System.err.println("Starting Job 4: Final Similarity");
        Job j4 = Job.getInstance(conf, "DIRT_4_FinalSim");
        j4.setJarByClass(DirtDriver.class);
        addAllPartsToCache(j4, conf, new Path(out25));
        j4.setMapperClass(Job4_FinalSim.Map.class);
        j4.setReducerClass(Job4_FinalSim.Reduce.class);
        j4.setOutputKeyClass(Text.class);
        j4.setOutputValueClass(DoubleWritable.class);
        j4.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j4, new Path(out3));
        FileOutputFormat.setOutputPath(j4, new Path(out4));

        boolean success = j4.waitForCompletion(true);
        System.err.println("Job completed: " + (success ? "SUCCESS" : "FAILURE"));
        return success ? 0 : 1;
    }

    // --- HELPER METHODS ---

    private void addCacheFilesWithPrefix(Job job, Configuration conf, Path parentDir, String prefix)
            throws IOException {
        FileSystem fs = parentDir.getFileSystem(conf);
        int count = 0;
        if (fs.exists(parentDir)) {
            FileStatus[] stats = fs.listStatus(parentDir);
            for (FileStatus stat : stats) {
                if (stat.getPath().getName().startsWith(prefix)) {
                    job.addCacheFile(stat.getPath().toUri());
                    count++;
                }
            }
        }
        System.err.println("Added " + count + " cache files with prefix: " + prefix);
    }

    private void addAllPartsToCache(Job job, Configuration conf, Path dir) throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        int count = 0;
        if (fs.exists(dir)) {
            FileStatus[] stats = fs.listStatus(dir);
            for (FileStatus st : stats) {
                if (st.getPath().getName().startsWith("part-")) {
                    job.addCacheFile(st.getPath().toUri());
                    count++;
                }
            }
        }
        System.err.println("Added " + count + " part files to cache from: " + dir);
    }

    private long readTotalN(Configuration conf, Path parentDir, String prefix) throws IOException {
        long sum = 0;
        FileSystem fs = parentDir.getFileSystem(conf);
        int fileCount = 0;
        
        if (fs.exists(parentDir)) {
            FileStatus[] stats = fs.listStatus(parentDir);
            for (FileStatus stat : stats) {
                if (stat.getPath().getName().startsWith(prefix)) {
                    fileCount++;
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stat.getPath())))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] partsStr = line.split("\t");
                            if (partsStr.length >= 2) {
                                try {
                                    sum += Long.parseLong(partsStr[1]);
                                } catch (NumberFormatException e) {
                                    System.err.println("WARNING: Cannot parse count in line: " + line);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if (fileCount == 0) {
            throw new IOException("No files found with prefix: " + prefix + " in " + parentDir);
        }
        
        System.err.println("Read total N from " + fileCount + " files: " + sum);
        return sum == 0 ? 1 : sum;
    }
}
