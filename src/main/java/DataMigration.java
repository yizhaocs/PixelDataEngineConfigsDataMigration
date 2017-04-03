import org.apache.commons.lang3.text.StrTokenizer;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/*

1:replacing all split1/split2 with split
2:flip the order of token rule
3:flp the order of set rule
4:flip the order of map rule
5:replacing tokenize with set // manual changed
6:replacing “map|tableName|keyId:position” with “map:tableName|keyId:position”  // manual changed
7:modified all condition rules from “ruleName|position:targetString” to “ruleName|keyid:position:targetString”
*/

public class DataMigration {
    public static void main(String[] args){
        DataMigration mDataMigration = new DataMigration();
        try {
            mDataMigration.scanLineByLine();
        }catch(Exception e){
            System.out.println(e);
        }
    }

    public void scanLineByLine() throws IOException {
        List<PixelDataEngineConfig> table = new ArrayList<PixelDataEngineConfig>();

        StrTokenizer tabSpliter = StrTokenizer.getCSVInstance();
        tabSpliter.setDelimiterChar('\t');
        StrTokenizer verticalBarSpliter = StrTokenizer.getCSVInstance();
        verticalBarSpliter.setDelimiterChar('|');
        StrTokenizer colonSpliter = StrTokenizer.getCSVInstance();
        colonSpliter.setDelimiterChar(':');


        Scanner s = new Scanner(new File("/Users/yzhao/Desktop/comparsion_test/sql_migration/pixel_data_engine_configs.csv"));

        /**
         * scan the file line by line
         */
        while(s.hasNextLine()){
            /**
             * read line
             */
            String line = s.nextLine();
            String[] array = tabSpliter.reset(line).getTokenArray();
            String gid = array[0];
            String key_id = array[1];
            String priority = array[2];
            String type = array[3];
            String parserRuleString = array[4];
            String conditionRuleString = array[5];
            String actionRuleString = array[6];
            String python_code = array[7];
            String modification_ts = array[8];

            // get the parserRule
            String parserRuleSplit[] = parserRuleString.split("\\|", 2);
            String parserRuleName = parserRuleSplit[0];
            List<String> parserTargetKeyStringList = null;
            if(parserRuleSplit.length == 2){
                parserTargetKeyStringList = verticalBarSpliter.reset(parserRuleSplit[1]).getTokenList();
            }

            if(parserRuleName.equals("split1")){
                parserRuleName = "split";
            }else if(parserRuleName.equals("split2")){
                parserRuleName = "split";
            } else if(parserRuleName.equals("token")){
                Collections.reverse(parserTargetKeyStringList);
            }

            parserRuleString = parserRuleName;
            if(parserTargetKeyStringList != null) {
                parserRuleString += "|";
                for (int i = 0; i < parserTargetKeyStringList.size(); i++) {
                    if(parserTargetKeyStringList.get(i).equals("|")){
                        if (i + 1 < parserTargetKeyStringList.size()) {
                            parserRuleString += "\\\"" + "|" + "\\\"" + "|";
                        }else{
                            parserRuleString += "\\\"" + "|" + "\\\"";
                        }
                    }else {
                        if (i + 1 < parserTargetKeyStringList.size()) {
                            parserRuleString += parserTargetKeyStringList.get(i) + "|";
                        } else {
                            parserRuleString += parserTargetKeyStringList.get(i);
                        }
                    }
                }
            }


            // get the conditionRule
            String conditionRuleSplit[] = conditionRuleString.split("\\|", 2);
            String contionRuleName = conditionRuleSplit[0];
            String[] conditionTargetKeyStringList = null;

            conditionRuleString = contionRuleName;
            if(conditionRuleSplit.length == 2){
                conditionTargetKeyStringList = verticalBarSpliter.reset(conditionRuleSplit[1]).getTokenArray();
                if(conditionTargetKeyStringList.length > 0){
                    conditionRuleString += "|";
                }
                for(int i = 0; i < conditionTargetKeyStringList.length; i++){
                    if(contionRuleName.equals("seg")){
                        conditionTargetKeyStringList[i] = key_id + ":a:" + conditionTargetKeyStringList[i];
                    }else {
                        conditionTargetKeyStringList[i] = key_id + ":" + conditionTargetKeyStringList[i];
                    }
                    if(i + 1 < conditionTargetKeyStringList.length){
                        conditionRuleString += conditionTargetKeyStringList[i] + "|";
                    }else{
                        conditionRuleString += conditionTargetKeyStringList[i];
                    }

                }
            }



            // get the actionRule
            String actionRuleSplit[] = actionRuleString.split("\\|", 2);
            String actionRuleName = actionRuleSplit[0];
            List<String> actionTargetKeyStringList = null;

            actionRuleString = actionRuleName;
            if(actionRuleSplit.length == 2){
                actionTargetKeyStringList = verticalBarSpliter.reset(actionRuleSplit[1]).getTokenList();
//                if(actionRuleName.equals("set") || actionRuleName.contains("map:")){
//                    Collections.reverse(actionTargetKeyStringList);
//                }
                if(actionTargetKeyStringList.size() > 0){
                    actionRuleString += "|";
                }
                for(int i = 0; i < actionTargetKeyStringList.size(); i++){
                    if (i + 1 < actionTargetKeyStringList.size()) {
                        if(actionRuleName.equals("set") || actionRuleName.contains("map:")){
                            List<String> tmp = colonSpliter.reset(actionTargetKeyStringList.get(i)).getTokenList();
                            actionRuleString += tmp.get(1) + ":" + tmp.get(0) + "|";
                        }else {
                            actionRuleString += actionTargetKeyStringList.get(i) + "|";
                        }
                    } else {
                        if(actionRuleName.equals("set") || actionRuleName.contains("map:")){
                            List<String> tmp = colonSpliter.reset(actionTargetKeyStringList.get(i)).getTokenList();
                            actionRuleString += tmp.get(1) + ":" + tmp.get(0);
                        }else {
                            actionRuleString += actionTargetKeyStringList.get(i);
                        }

                    }
                }

            }

            //
            PixelDataEngineConfig row = new PixelDataEngineConfig();
            row.setGid(gid);
            row.setKey_id(key_id);
            row.setPriority(priority);
            row.setType(type);
            row.setParse_rule(parserRuleString);
            row.setCondition_rule(conditionRuleString);
            row.setAction_rule(actionRuleString);
            row.setPython_code(python_code);
            row.setModification_ts(modification_ts);
            table.add(row);



        }






        /**
         * create the sql for patch
         */
        FileWriter out = null;
        try{
            out = new FileWriter("/Users/yzhao/Desktop/output.txt");
            for(PixelDataEngineConfig row: table){
                String updateQuery = "UPDATE pixel_data_engine_configs SET ";
                updateQuery += "parse_rule=\"" + row.getParse_rule() + "\"" + ", ";
                updateQuery += "condition_rule=\"" + row.getCondition_rule() + "\"" + ", ";
                updateQuery += "action_rule=\"" + row.getAction_rule() + "\"" + " ";



                updateQuery += "WHERE gid=" + row.getGid() + " AND key_id=\"" + row.getKey_id() + "\"" + " AND priority=" + row.getPriority();
                out.write(updateQuery);
                out.write(";");
                out.write("\n");
                //System.out.println(row.getCondition_rule());
            }
        }catch (Exception e){

        } finally {
            if (out != null) {
                out.close();
            }
        }
    }


    private class PixelDataEngineConfig{
        private String gid;
        private String key_id;
        private String priority;
        private String type;
        private String parse_rule;
        private String condition_rule;
        private String action_rule;
        private String python_code;
        private String modification_ts;

        public String getGid() {
            return gid;
        }

        public void setGid(String gid) {
            this.gid = gid;
        }

        public String getKey_id() {
            return key_id;
        }

        public void setKey_id(String key_id) {
            this.key_id = key_id;
        }

        public String getPriority() {
            return priority;
        }

        public void setPriority(String priority) {
            this.priority = priority;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getParse_rule() {
            return parse_rule;
        }

        public void setParse_rule(String parse_rule) {
            this.parse_rule = parse_rule;
        }

        public String getCondition_rule() {
            return condition_rule;
        }

        public void setCondition_rule(String condition_rule) {
            this.condition_rule = condition_rule;
        }

        public String getAction_rule() {
            return action_rule;
        }

        public void setAction_rule(String action_rule) {
            this.action_rule = action_rule;
        }

        public String getPython_code() {
            return python_code;
        }

        public void setPython_code(String python_code) {
            this.python_code = python_code;
        }

        public String getModification_ts() {
            return modification_ts;
        }

        public void setModification_ts(String modification_ts) {
            this.modification_ts = modification_ts;
        }
        /*
        @Override
        public String toString(){

        }
*/
        private class RuleElement<E>{


        }



        private class LenCondition{
            private String keyId;
            private String position;
            private String targetKeyString;

            public String getKeyId() {
                return keyId;
            }

            public void setKeyId(String keyId) {
                this.keyId = keyId;
            }

            public String getPosition() {
                return position;
            }

            public void setPosition(String position) {
                this.position = position;
            }

            public String getTargetKeyString() {
                return targetKeyString;
            }

            public void setTargetKeyString(String targetKeyString) {
                this.targetKeyString = targetKeyString;
            }
        }


    }
}
