package udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class UDFRegister {
    public static void  registerAllCustomUDFS(SparkSession spark) {

        UDF1<Integer,Double> apheight = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {

                if(d<=85.0)
                    return -0.352215044;
                else if(d<=352.0)
                    return -0.009858368491;
                else if(d<=470.0)
                    return 1.051969267;
                else if(d<=520.0)
                    return 1.132039987;
                else if(d<=561.0)
                    return 1.130653436;
                else if(d<=631.0)
                    return 1.018866633;
                else if(d<=1012.0)
                    return 0.8620427863;
                else if(d<=2194.0)
                    return -0.1219548362;
                else if(d<=2471.0)
                    return -0.6730404346;
                else if(d<=2719.0)
                    return -0.8900676924;
                else
                    return -1.035325453;
            }
        };

        UDF1<Integer,Double> bsheight = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {

                if(d<=512.0)
                    return -0.6929527603;
                else if(d<=561.0)
                    return -0.3828760252;
                else if(d<=603.0)
                    return -0.2057396285;
                else if(d<=627.0)
                    return -0.1306199545;
                else if(d<=672.0)
                    return -0.05845499092;
                else if(d<=694.0)
                    return -0.0240320363;
                else if(d<=717.0)
                    return -0.003919406471;
                else if(d<=785.0)
                    return 0.007578521682;
                else if(d<=828.0)
                    return 0.1216590298;
                else if(d<=939.0)
                    return 0.1547005775;
                else if(d<=957.0)
                    return 0.2369341453;
                else if(d<=1070.0)
                    return 0.2384670372;
                else
                    return 0.3163398307;

            }
        };

        UDF1<Integer,Double> rpheight = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if(d<= -541.0)
                    return -1.9900234614;
                else if(d<=-58.0)
                    return -1.166285515;
                else if(d<= 85.0)
                    return 0.1283180937;
                else if(d<=105.0)
                    return 1.4101692265;
                else if(d<=561.0)
                    return 1.147967377;
                else if(d<=657.0)
                    return 1.054043688;
                else if(d<=849.0)
                    return 0.6028041782;
                else if(d<=1642.0)
                    return -0.1602094396;
                else if(d<=2075.0)
                    return -0.6217132316;
                else if(d<=2454.0)
                    return -0.9555696116;
                else if(d<=2805.0)
                    return -1.216757086;
                else
                    return -1.392072325;

            }
        };

        UDF1<Integer,Double> apWidth = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if(d<=10.0)
                    return -0.5239919566;
                else if(d<=15.0)
                    return 0.0936469981;
                else if(d<=38.0)
                    return -0.3015218218;
                else if(d<=127.0)
                    return 0.2601852299;
                else if(d<=247.0)
                    return 0.3171288291;
                else if(d<=709.0)
                    return -0.1336799855;
                else if(d<=732.0)
                    return -0.2381379746;
                else if(d<=880.0)
                    return -0.0609518648;
                else if(d<=948.0)
                    return -0.0122259931;
                else if(d<=965.0)
                    return -0.1101735305;
                else if(d<=1034.0)
                    return -0.0269030452;
                else if(d<=1082.0)
                    return -0.0333381488;
                else if(d<=1159.0)
                    return -0.0249311865;
                else if(d<=1197.0)
                    return 0.0223849564;
                else if(d<=1307.0)
                    return 0.0227488108;
                else if(d<=1357.0)
                    return 0.1346032314;
                else
                    return 0.2194443298;

            }
        };

        UDF1<Integer,Double> bsWidth = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if(d<=375.0)
                    return -0.6055915491;
                else if(d<=785.0)
                    return -0.55375862;
                else if(d<=1024.0)
                    return -0.3044384433;
                else if(d<=1093.0)
                    return -0.0707638504;
                else if(d<=1187.0)
                    return 0.0350039884;
                else if(d<=1260.0)
                    return 0.067517545;
                else if(d<=1280.0)
                    return 0.0430563798;
                else if(d<=1353.0)
                    return 0.1134398106;
                else if(d<=1366.0)
                    return 0.0400224197;
                else if(d<=1374.0)
                    return 0.1120139535;
                else if(d<=11440.0)
                    return 0.1108844876;
                else if(d<=1536.0)
                    return 0.1297185277;
                else if(d<=1600.0)
                    return 0.1422317216;
                else if(d<=1680.0)
                    return 0.1692737079;
                else if(d<=1855.0)
                    return 0.2000845844;
                else if(d<=1920.0)
                    return 0.2769310449;
                else
                    return 0.3715206918;

            }
        };

        UDF1<Integer,Double> rpWidth = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if(d<=10.0)
                    return -0.551678737;
                else if(d<=15.0)
                    return 0.1047669992;
                else if(d<=38.0)
                    return -0.3012158541;
                else if(d<=122.0)
                    return 0.2518776686;
                else if(d<=1247.0)
                    return 0.3344543069;
                else if(d<=709.0)
                    return -0.1447618256;
                else if(d<=727.0)
                    return -0.2538630071;
                else if(d<=875.0)
                    return -0.0591136817;
                else if(d<=948.0)
                    return -0.0084428006;
                else if(d<=965.0)
                    return -0.1456458505;
                else if(d<=1034.0)
                    return -0.0248892508;
                else if(d<=1076.0)
                    return -0.0197491316;
                else if(d<=1158.0)
                    return -0.0046244803;
                else if(d<=1197.0)
                    return 0.0300678436;
                else if(d<=1304.0)
                    return 0.0215945176;
                else if(d<=1357.0)
                    return 0.1353111506;
                else
                    return 0.2144018033;

            }
        };

        UDF1<String,Double> art = new UDF1<String, Double>() {
            @Override
            public Double call(String d) throws Exception {
                if(d.equals("o"))
                    return -0.1174517543;
                else if(d.equals("t"))
                    return 2.0225235241;
                else
                    return -0.2070193112;

            }
        };


        UDF1<Integer,Double> arc = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if(d<=1.0)
                    return -0.0497566819;
                else
                    return 1.6756226612;


            }
        };

        UDF1<Integer,Double> ivp = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if (d <= 10.0)
                    return -0.66632552;
                else if (d <= 40.0)
                    return 0.6145088846;
                else if (d <= 60.0)
                    return 0.7645581141;
                else if (d <= 80.0)
                    return 0.9625531948;
                else
                    return 0.6884156944;
            }

        };

        UDF1<Integer,Double> pos = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if(d<=2.0)
                    return 0.2084552874;
                else if(d<=3.0)
                    return -1.1631909884;
                else
                    return -0.4144602252;
            }
        };

        UDF1<Integer,Double> tile = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if(d<=2.0)
                    return -0.3745073189;
                else if(d<=3.0)
                    return 0.7782103858;
                if(d<=4.0)
                    return 0.8259665723;
                else if(d<=5.0)
                    return 0.1160359501;
                if(d<=6.0)
                    return -0.1634690729;
                else if(d<=7.0)
                    return -1.2078553707;
                else
                    return -0.373189672;
            }
        };



        UDF1<Integer,Double> fp = new UDF1<Integer, Double>() {
            @Override
            public Double call(Integer d) throws Exception {
                if(d==0)
                    return -0.6886801994;
                else
                    return 0.699522288108179;


            }
        };


        UDF1<String,Double> CreativeSize = new UDF1<String, Double>() {
            @Override
            public Double call(String d) throws Exception {
                if(d.equals("160x600"))
                    return 1.0699133562668;
                else if(d.equals("300x1050"))
                    return 0.569042406633131;
                else if(d.equals("300x250"))
                    return -0.333110654569169;
                else if(d.equals("300x600"))
                    return 0.333900378369031;
                else if(d.equals("728x90"))
                    return -0.316339108936361;
                else if(d.equals("970x250"))
                    return -0.414741126576713;
                else
                    return -0.548431284330724;


            }
        };

        UDF1<String,Double> DeviceCategory = new UDF1<String, Double>() {
            @Override
            public Double call(String d) throws Exception {
                if(d.equals("d"))
                    return 0.106863159620783;
                else if(d.equals("m"))
                    return -0.564364161429825;
                else if(d.equals("others"))
                    return -0.890312682636003;
                else if(d.equals("t"))
                    return -0.302132178977278;
                else if(d.equals("u"))
                    return -0.142345515174896;
                else
                    return 0.362450285859365;

            }
        };





        spark.udf().register("apheight", apheight, DataTypes.DoubleType);

        spark.udf().register("bsheight", bsheight, DataTypes.DoubleType);

        spark.udf().register("rpheight", rpheight, DataTypes.DoubleType);

         spark.udf().register("apWidth", apWidth, DataTypes.DoubleType);

        spark.udf().register("bsWidth", bsWidth, DataTypes.DoubleType);

        spark.udf().register("rpWidth", rpWidth, DataTypes.DoubleType);

         spark.udf().register("art", art, DataTypes.DoubleType);

        spark.udf().register("arc", arc, DataTypes.DoubleType);

        spark.udf().register("ivp", ivp, DataTypes.DoubleType);
         spark.udf().register("pos", pos, DataTypes.DoubleType);

        spark.udf().register("tile", tile, DataTypes.DoubleType);

        spark.udf().register("fp", fp, DataTypes.DoubleType);
        spark.udf().register("CreativeSize", CreativeSize, DataTypes.DoubleType);

        spark.udf().register("DeviceCategory", DeviceCategory, DataTypes.DoubleType);

           }
}
