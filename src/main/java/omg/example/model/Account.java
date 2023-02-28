package omg.example.model;

public class Account {
    String name;
    int point;
    int transactionCount;
    public Account(String name) {
        this.name = name;
        this.point = 0;
        transactionCount = 0;
    }
    public String name(){return name;}
    public void changePoint(int point){
        this.point += point;
        transactionCount++;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Account) //old java instanceof
            return this.name.equals(((Account) obj).name());
        return false;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", point=" + point +
                ", transactionCount=" + transactionCount +
                '}';
    }
}
