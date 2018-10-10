package threading;


import model.DataPojo;
import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.RecursiveTask;

public class PartMerger extends RecursiveTask<String> {
    private static RandomStringGenerator generator=new RandomStringGenerator.Builder()
            .withinRange('0', 'z')
            .filteredBy(CharacterPredicates.LETTERS, CharacterPredicates.DIGITS).build();

    private final List<String> parts;

    public PartMerger(List<String> parts) {
        super();
        this.parts = parts;
    }

    private static String stitchFiles(String leftFile, String rightFile) throws IOException {

        String resFile="./src/main/resources/merged/"+generator.generate(5)+"_stitch.csv";

        try (FileInputStream inputStream1 = new FileInputStream(leftFile);
             FileInputStream inputStream2 = new FileInputStream(rightFile);
             Scanner sc1 = new Scanner(inputStream1, "UTF-8");
             Scanner sc2 = new Scanner(inputStream2, "UTF-8");
             BufferedWriter writer = Files.newBufferedWriter(Paths.get(resFile))) {

            //INIT PHASE
            DataPojo data1=null, data2=null;
            String line1="",line2="";
            if(sc1.hasNextLine()) {
                line1=sc1.nextLine();
                data1 = new DataPojo(line1);
            }
            if(sc2.hasNextLine()) {
                line2=sc1.nextLine();
                data2 = new DataPojo(line2);
            }

            //ZIPPING 2 files into one
            while (data1!=null || data2!=null ) {

                if(data1==null) {
                   writer.append(line2).append("\n");
                   while(sc2.hasNextLine()) writer.append(sc2.nextLine()).append("\n");
                   data2=null;
                } else if(data2==null) {
                    writer.append(line1).append("\n");
                    while(sc1.hasNextLine()) writer.append(sc1.nextLine()).append("\n");
                    data1=null;
                } else if(data1.compareTo(data2)>0) {
                    writer.append(line2).append("\n");
                    data2=null;
                    if(sc2.hasNextLine()) {
                        line2=sc2.nextLine();
                        data2 = new DataPojo(line2);
                    }
                } else {
                    writer.append(line1).append("\n");
                    data1=null;
                    if(sc1.hasNextLine()) {
                        line1=sc1.nextLine();
                        data1 = new DataPojo(line1);
                    }
                }
            }
            // note that Scanner suppresses exceptions
            if (sc1.ioException() != null) {
                throw sc1.ioException();
            }
            if (sc2.ioException() != null) {
                throw sc2.ioException();
            }
        }
        //CLEAN UP
        Files.delete(Paths.get(leftFile));
        Files.delete(Paths.get(rightFile));
        System.out.println(String.format("<><>MERGED %s and %s into %S",leftFile,rightFile,resFile));
        return resFile;
    }


    @Override
    protected String compute() {
        String mergedFile = "";

        if (parts.size() > 2) {
            PartMerger taskLeft = new PartMerger(parts.subList(0, parts.size() / 2));
            PartMerger taskRigth = new PartMerger(parts.subList(parts.size() / 2, parts.size()));
            taskLeft.fork();
            taskRigth.fork();
            try {
                mergedFile = stitchFiles(taskLeft.join(), taskRigth.join());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (parts.size() == 2) {
            //DO the actual merge
            try {
                mergedFile = stitchFiles(parts.get(0), parts.get(1));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (parts.size() == 1) {
            //odd number of parts - merge with final = return the name as result
            mergedFile = parts.get(0);
        }
        return mergedFile;
    }
}
