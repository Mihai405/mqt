package hw5_2;

public class Order {
    private String comanda;
    private String client_id;
    private String numar_produse;

    public Order(String comanda, String client_id, String numar_produse) {
        this.comanda = comanda;
        this.client_id = client_id;
        this.numar_produse = numar_produse;
    }

    public String getComanda() {
        return comanda;
    }

    public void setComanda(String comanda) {
        this.comanda = comanda;
    }

    public String getClient_id() {
        return client_id;
    }

    public void setClient_id(String client_id) {
        this.client_id = client_id;
    }

    public String getNumar_produse() {
        return numar_produse;
    }

    public void setNumar_produse(String numar_produse) {
        this.numar_produse = numar_produse;
    }
}
