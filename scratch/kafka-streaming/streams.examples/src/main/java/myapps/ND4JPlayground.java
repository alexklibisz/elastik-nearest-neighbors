import org.datavec.image.loader.NativeImageLoader;
import org.nd4j.linalg.api.ndarray.INDArray;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.FileInputStream;


public class ND4JPlayground {
    public static void main(String[] args) throws Exception {

        final String imgPath = "/home/alex/tmp/test.jpg";

        FileInputStream fs = new FileInputStream(imgPath);
//        BufferedInputStream bs = new BufferedInputStream(fs);
//        BufferedImage bimg = ImageIO.read(bs);

        NativeImageLoader loader = new NativeImageLoader();
        INDArray img = loader.asMatrix(fs);

        System.out.println(img);

//        INDArray img = INDArray()

//        System.out.println(bs);
//        System.out.println(bimg);

    }
}
