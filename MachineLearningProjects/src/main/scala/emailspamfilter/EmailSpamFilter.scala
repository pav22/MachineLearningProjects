package emailspamfilter

object EmailSpamFilter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("EmailSpam").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    println("HC build")

    val spamMails = sc.textFile("/home/cloudera/DataSets/spam_filtering/spam")

    val hamMails = sc.textFile("/home/cloudera/DataSets/spam_filtering/ham")

    val features = new HashingTF(numFeatures = 1000)

    val Features_spam = spamMails.map(mail => features.transform(mail.split(" ")))

    val Features_ham = hamMails.map(mail => features.transform(mail.split(" ")))

    val positiveData = Features_spam.map(features => LabeledPoint(1, features))

    val negativeData = Features_ham.map(features => LabeledPoint(0, features))
    val data = positiveData.union(negativeData)

    println("Before cache")
    data.cache()

    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
    val logistic_Learner = new LogisticRegressionWithSGD()
    println("Going to  model")
    val model = logistic_Learner.run(training)

    val predictionLabel = test.map(x => (model.predict(x.features), x.label))

    val accuracy = 1.0 * predictionLabel.filter(x => x._1 == x._2).count() / training.count()
    println("Accuracy : " + accuracy)

  }

}