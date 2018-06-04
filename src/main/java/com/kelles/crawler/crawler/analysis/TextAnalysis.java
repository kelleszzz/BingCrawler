package com.kelles.crawler.crawler.analysis;

import java.math.BigInteger;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.kelles.crawler.crawler.setting.Setting;
import com.kelles.crawler.crawler.util.Logger;
import org.ansj.domain.Result;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.nlpcn.commons.lang.tire.library.Library;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.AnnotationPipeline;
import edu.stanford.nlp.pipeline.TokenizerAnnotator;
import edu.stanford.nlp.util.CoreMap;


public class TextAnalysis {

    public static final int totalBits = 32;

    public static void main(String[] args) {
        String str = "央广网11月12日报道，据中国之声《新闻晚高峰》报道，眼下这个周末，对于韩国在野党以及其他一些反对总统朴槿惠的人士来说，将又是一个不休之日。就在大约一个小时前，北京时间今天（12日）下午5点开始，由韩国1500多个公民团体组织要求朴槿惠下台的游行以及烛光集会再次举行。目前，示威者们采取“隔空包围总统府青瓦台”的形式，预计集会将持续到明天。\n" +
                "\n" +
                "　　对此，青瓦台方面密切关注，而有分析认为，此次集会将成为当前韩国政治局面的重要分水岭，集会规模和影响力或将决定“亲信干政事件”的事态走向。\n" +
                "\n" +
                "　　据中国国际广播电台驻韩国首尔记者杨宁介绍，韩国劳工届、女权界、学界人士、青少年、大学生今天下午2点在大学路、钟路、南大门、首尔站、首尔广场等地先期举行集会。下午4点聚集到首尔广场，下午5点进行大规模示威游行活动，游行以隔空包围总统府青瓦台的形式进行。示威人群从首尔广场出发，途径钟路、西大门等地。游行结束之后，晚上7点在光化门一带将举行搭帐篷静坐示威，公民自由发言等活动，活动将持续到朴槿惠到次日。\n" +
                "\n" +
                "　　据悉，韩国三大在野党领导核心和所属的议员将大批参加今天的集会。共同民主党党首等领导核心全体决定参加集会，国民之党早就确定参加。正义党从一开始就跟市民团体一起参加了烛光集会。在野阵营的下一届总统选举有力候选人也将大批亮相。分析认为，在野阵营一同参加今天的集会，一是朴槿惠按照在野党要求完全退居二线，另一方面今天的集会若没有取得预期效果，在野党可能改变立场，与执政党将国会推荐新总理人选问题启动谈判。\n" +
                "\n" +
                "　　民调机构“盖勒普韩国”11号公布的调查结果，韩国民众对总统朴槿惠的支持率连续两周保持5%，也就是说，仅有5%的受访者对朴槿惠施政给予正面评价，相反给予负面评价的比重为90%，较前一周上升一个百分点，再创新低。值得关注的是20岁-30岁年龄段的受访者，对朴槿惠的支持率为0%。此外执政党、新国家党的支持率也下滑至17%，这不仅是自朴槿惠就任以来的最低职，也是新国家党前身、大国家党成立以来的历史新低。\n" +
                "\n" +
                "　　和前两次的抗议活动一样，首尔的抗议活动组织者威胁说，只要朴槿惠一天不下台，他们就每晚在首尔市中心举行抗议活动。今天的这场集会他们就称之为 “要求朴槿惠政府下台民众总动员”大会。\n" +
                "\n" +
                "　　目前韩国检方对朴槿惠“亲信干政”事件的调查不断深入，同时在野党也对朴槿惠步步紧逼。在朴槿惠一系列让步未能平息风波的背景下，在野党对其发出最后通牒，朴槿惠的政治命运面临重大考验。\n" +
                "\n" +
                "　　对于目前纷繁复杂的韩国政局，外界认为朴槿惠已表现出力不从心。她下一步将如何行动，还有待观察。韩国在野的共同民主党和国民之党高层强硬表示，如朴槿惠到12日集会当天还不宣布“退居二线”，两党只能着手强力推动朴槿惠下台。\n" +
                "\n" +
                "　　有西方分析人士认为，由于朴槿惠并未表露出主动放弃权力的决心，目前情况下，她主动下台的可能性不大。不过，为安抚民心，朴槿惠很可能打出“退出执政党”这张牌。彭博社东亚分析师约翰傅朗认为，这样做一方面可以切割与执政党的关系，表现出总统实际上退居二线的决心，来弱化在野党的声讨；另一方面也可以顺应党内“非朴派”的要求，挽救支持率日益下滑的执政党。\n" +
                "\n" +
                "　　分析人士称，即使朴槿惠下台，法律规定60天内就需要举行大选，但眼下又很难找出合适的总统候选人。或许可以通过组建“过渡内阁”的方式，将原本应于明年底举行的大选提前至明年4月举行，以实现国政平稳过渡。\n" +
                "\n" +
                "　　随着事件的发酵，更多细节陆续披露，很多韩国民众感到震惊、愤怒的同时，也对记者表示，事件也让人们产生更多思考，更重要的是，未来怎样避免这样的事再次发生。不仅是总统，还有国会议员等政治人物，都应该更好接受监督，多多与国民沟通。希望韩国社会让民意更多的发声，让政治人物顺应民意。\n" +
                "\n" +
                "　　日前青瓦台总统府相关人士称，总统会将治理国政职权下放给“国会推荐的总理”。有人表示，这可以理解为朴槿惠实际将退居二线。但不少观察人士仍然认为，韩国宪法规定，总理必须根据总统的命令来统辖各行政部门，这意味着总统仍将掌握对总理的约束权。因此存在今后免去总理并解散内阁的可能。";
        String str2 = "特朗普和他的儿子埃里克（左一）、小唐纳德（左二）和女儿伊万卡（右一）。（图片来源：美国媒体）\n" +
                "\n" +
                "美国新当选总统唐纳德·特朗普尚未正式宣誓就职，就已着手进行首次人员大调整。11月11日，特朗普表示，他已任命竞选搭档、新当选副总统迈克·彭斯负责挑选新一届政府官员等过渡时期事宜。有媒体调侃，特朗普当选总统像是一场“家族盛事”，他的两个儿子和女儿女婿都在过渡小组中占据要职。\n" +
                "\n" +
                "组长换人\n" +
                "\n" +
                "据英国《每日邮报》网站报道，特朗普11日发表声明宣布对其过渡小组进行人员调整，任命彭斯担任组长，原组长、新泽西州州长克里斯·克里斯蒂留任副组长。\n" +
                "\n" +
                "美国《纽约时报》援引数名过渡小组成员的话说，彭斯将负责挑选新一届政府官员等过渡时期事宜。而特朗普之所以选择彭斯，是希望利用后者在华盛顿的资源和人脉更好地推动过渡小组的工作。\n" +
                "\n" +
                "当天早些时候，特朗普在社交网站推特（Twitter）上发文：“今天在纽约的日程安排很繁忙。将会很快做出一些重要决定，来确定由谁来管理我们的政府。”\n" +
                "\n" +
                "全家上阵\n" +
                "\n" +
                "《每日邮报》注意到，特朗普的两个儿子——小唐纳德和埃里克，女儿伊万卡和女婿贾瑞德·库斯纳都成为过渡小组成员。\n" +
                "\n" +
                "纽约州众议员克里斯·科林斯是首名表态支持特朗普的国会议员，他11日接受美国有线电视新闻网（CNN）采访时透露，特朗普新政府白宫幕僚长的最终人选将在本周末得到确认。白宫幕僚长是美国总统办事机构的最高级别官员，同时亦是总统的高级助理。白宫幕僚长拥有很大的权力，常被称为“华盛顿具第二大权力的人”。\n" +
                "\n" +
                "《纽约时报》称，特朗普竞选团队首席执行官史蒂文·班农是白宫幕僚长的有力竞争者之一，据信他在竞选最后阶段贡献了不少“妙计”，比如邀请几名曾控诉遭希拉里的丈夫比尔·克林顿性侵的女性在总统竞选辩论前召开新闻发布会，可以说他为特朗普赢得大选立下了汗马功劳。\n" +
                "\n" +
                "特朗普的女婿库斯纳也被认为是白宫幕僚长备选人。库斯纳出身新泽西州一个地产巨贾之家，他本身是一位媒体大亨，拥有《纽约观察报》。据称特朗普对这个女婿青睐有加，《纽约客》杂志近日援引一名消息人士的话说库斯纳才是特朗普竞选活动的“真正负责人”。10日，库斯纳陪同特朗普前往白宫，与现任总统奥巴马和国会领导人见面。\n" +
                "\n" +
                "此外，特朗普过渡小组成员还包括硅谷亿万富翁彼得·蒂埃尔、前纽约市长鲁迪·朱利安尼、特朗普党内初选竞争对手本·卡尔森和多名国会议员。";
        String str3 = "The self-organizing exploratory pattern of the Argentine ant";
        String str4 = "The Self-organising Exploratory Pattern of the Argentine Ant";

        BigInteger simHash1 = getSimHash(str3), simHash2 = getSimHash(str4);
        Logger.log(simHash1);
        Logger.log(simHash2);
        Logger.log(hammingDistance(simHash1, simHash2));

    }

    //获取两个签名的海明距离
    public static int hammingDistance(BigInteger simHash1, BigInteger simHash2) {
        String binaryStr1, binaryStr2;
        binaryStr1 = SimHash.getBinaryStr(simHash1);
        binaryStr2 = SimHash.getBinaryStr(simHash2);
        return SimHash.getDistance(binaryStr1, binaryStr2);
    }

    static {
        initAnsj();
    } //初始化中文分词词典

    //通过纯内容文本获取内容签名
    public static BigInteger getSimHash(String text) {
        return getSimHash(text, false);
    }

    public static BigInteger getSimHash(String text, boolean onlyN) {
        if (text == null) return null;
        //文本内容分词,选取前150个名次
        Set<String> keyWords = new HashSet(); //自动排序
        Result terms = ToAnalysis.parse(text);
        for (int i = 0, len = terms.size(), keyWordsCount = 0; i < len && keyWordsCount <= 150; i++) {
            String natureStr = terms.get(i).getNatureStr();
            //英文分词
            if ("en".equals(natureStr)) {
                AnnotationPipeline pipeline = getAnnotationPipeline();
                Annotation annotation;
                annotation = new Annotation(terms.get(i).getName());
                pipeline.annotate(annotation);
                List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
                if (sentences != null && !sentences.isEmpty()) {
                    CoreMap sentence = sentences.get(0);
                    for (CoreMap token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                        String word = token.get(CoreAnnotations.TextAnnotation.class);
                        String lema = token.get(CoreAnnotations.LemmaAnnotation.class);//原型
                        String partOfSpeech = token.get(CoreAnnotations.PartOfSpeechAnnotation.class); //词性
                        if (onlyN) {
                            /*只收录名词*/
                            if (partOfSpeech.startsWith("N")) {
                                Logger.log(11.11, "收录了英文" + lema);
                                keyWords.add(lema);
                            }
                        } else keyWords.add(lema); //收录所有词
                    }
                }
            }
            //中文分词
            else {
                if (onlyN) {
                    if ((natureStr.startsWith("n") && !"null".equals(natureStr)) || natureStr.contains("define")) {
                        Logger.log(11.11, "收录了中文" + terms.get(i).getName() + "(词性" + terms.get(i).getNatureStr() + ")");
                        keyWords.add(terms.get(i).getName()); //只收录名词
                    }
                } else keyWords.add(terms.get(i).getName()); //收录所有词
            }

            keyWordsCount++;
        }
        Logger.log(11.11, "提取出关键词:\n" + keyWords + "");

        //计算64位签名
        List<String> keyWordsList = new ArrayList();
        keyWordsList.addAll(keyWords);
        SimHash hash1 = new SimHash(keyWordsList, 64);
        return hash1.intSimHash;
    }


    private static AnnotationPipeline getAnnotationPipeline() {
        // Add in sentiment
        AnnotationPipeline pipeline = new AnnotationPipeline();
        pipeline.addAnnotator(new TokenizerAnnotator(false, "en"));
//		 pipeline.addAnnotator(new WordsToSentencesAnnotator(false));
//		 pipeline.addAnnotator(new POSTaggerAnnotator(false));
//		 pipeline.addAnnotator(new MorphaAnnotator(false));
//		 pipeline.addAnnotator(new ParserAnnotator(false, -1));
        return pipeline;
    }

    //初始化中文分词词典
    private static boolean ifInitAnsj = false;

    private static void initAnsj() {
        // 构造一个用户词典
        try {
            if (!ifInitAnsj) {
                Library.makeForest(Setting.ANSJ_LIBRARY);
                ifInitAnsj = true;
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            Exception e1 = new Exception("将ansj_seg-master中的library文件夹移动到项目根目录下");
            e1.initCause(e);
            e1.printStackTrace();
        }
    }


    //SimHash类
    @Deprecated
    protected static class SimHash {
        private List<String> tokens;
        private BigInteger intSimHash;
        private int hashbits;

        public SimHash(List<String> tokens) {
            this(tokens, 64);
        }

        public SimHash(List<String> tokens, int hashbits) {
            this.tokens = tokens;
            this.hashbits = hashbits;
            calculateSimHash();
        }

        //BigInt的simHash转换为二进制String的simHash
        public static String getBinaryStr(BigInteger t) {
            return getBinaryStr(t, 64);
        }

        public static String getBinaryStr(BigInteger t, int hashbits) {
            if (t == null) return null;
            StringBuffer simHashBuffer = new StringBuffer();
            for (int i = hashbits - 1; i >= 0; i--) {
                BigInteger bitmask = new BigInteger("1").shiftLeft(i);
                if (t.and(bitmask).signum() != 0) {
                    simHashBuffer.append("1");
                } else {
                    simHashBuffer.append("0");
                }
            }
            return simHashBuffer.toString();
        }

        private void calculateSimHash() {
            int[] v = new int[this.hashbits];
            for (String token : this.tokens) {
                BigInteger t = this.hash(token);
                for (int i = 0; i < this.hashbits; i++) {
                    BigInteger bitmask = new BigInteger("1").shiftLeft(i);
                    if (t.and(bitmask).signum() != 0) {
                        v[i] += 1;
                    } else {
                        v[i] -= 1;
                    }
                }
            }
            BigInteger fingerprint = new BigInteger("0");
            StringBuffer simHashBuffer = new StringBuffer();
            for (int i = 0; i < this.hashbits; i++) {
                if (v[i] >= 0) {
                    fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
                    simHashBuffer.append("1");
                } else {
                    simHashBuffer.append("0");
                }
            }
            this.intSimHash = fingerprint;
        }

        //对给定文本求出哈希值
        private BigInteger hash(String source) {
            return hash(source, "utf-8");
        }

        private BigInteger hash(String source, String charset) {
            if (source == null || source.length() == 0) {
                return new BigInteger("0");
            }
            byte[] bytes = null;
            try {
                bytes = source.getBytes(charset);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            BigInteger x = BigInteger.valueOf(((long) bytes[0]) << 7);
            BigInteger m = new BigInteger("1000003");
            BigInteger mask = new BigInteger("2").pow(this.hashbits).subtract(new BigInteger("1"));
            for (byte b : bytes) {
                BigInteger temp = BigInteger.valueOf((long) b);
                x = x.multiply(m).xor(temp).and(mask);
            }
            x = x.xor(new BigInteger(String.valueOf(source.length())));
            if (x.equals(new BigInteger("-1"))) {
                x = new BigInteger("-2");
            }
            return x;
        }

        //和另一个SimHash求海明距离
        public int hammingDistance(SimHash other) {
            BigInteger x = this.intSimHash.xor(other.intSimHash);
            int tot = 0;

            while (x.signum() != 0) {
                tot += 1;
                x = x.and(x.subtract(new BigInteger("1")));
            }
            return tot;
        }

        //两个相同长度字符串求海明距离
        public static int getDistance(String str1, String str2) {
            int distance;
            if (str1 == null || str2 == null || str1.length() != str2.length()) {
                distance = -1;
            } else {
                distance = 0;
                for (int i = 0; i < str1.length(); i++) {
                    if (str1.charAt(i) != str2.charAt(i)) {
                        distance++;
                    }
                }
            }
            return distance;
        }
    }


}
