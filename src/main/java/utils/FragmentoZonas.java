package utils;

import java.util.HashMap;
import java.util.Map;

public class FragmentoZonas {
    private static final Map<String, String> mapaEstadosZona = new HashMap<>();

    static {
        // 🔹 Mapeo de estados a zonas según la imagen
        mapaEstadosZona.put("Durango", "ZonaNorte");
        mapaEstadosZona.put("CDMX", "ZonaNorte");
        mapaEstadosZona.put("Chihuahua", "ZonaNorte");
        mapaEstadosZona.put("Estado de Mexico", "ZonaNorte");
        mapaEstadosZona.put("Baja California", "ZonaNorte");
        mapaEstadosZona.put("Queretaro", "ZonaNorte");
        mapaEstadosZona.put("Coahuila", "ZonaNorte");
        mapaEstadosZona.put("Tamaulipas", "ZonaNorte");
        mapaEstadosZona.put("Sonora", "ZonaNorte");
        mapaEstadosZona.put("Morelos", "ZonaNorte");

        mapaEstadosZona.put("Zacatecas", "ZonaCentro");
        mapaEstadosZona.put("Hidalgo", "ZonaCentro");
        mapaEstadosZona.put("Queretaro", "ZonaCentro");
        mapaEstadosZona.put("Puebla", "ZonaCentro");
        mapaEstadosZona.put("Nuevo Leon", "ZonaCentro");
        mapaEstadosZona.put("CDMX", "ZonaCentro");
        mapaEstadosZona.put("Morelos", "ZonaCentro");
        mapaEstadosZona.put("Yucatan", "ZonaCentro");
        mapaEstadosZona.put("San Luis Potosi", "ZonaCentro");
        mapaEstadosZona.put("Michoacan", "ZonaCentro");

        mapaEstadosZona.put("Oaxaca", "ZonaSur");
        mapaEstadosZona.put("Veracruz", "ZonaSur");
        mapaEstadosZona.put("Guerrero", "ZonaSur");
        mapaEstadosZona.put("Zacatecas", "ZonaSur");
        mapaEstadosZona.put("Quintana Roo", "ZonaSur");
        mapaEstadosZona.put("Queretaro", "ZonaSur");
        mapaEstadosZona.put("Guanajuato", "ZonaSur");
        mapaEstadosZona.put("Estado de Mexico", "ZonaSur");
        mapaEstadosZona.put("Campeche", "ZonaSur");
        mapaEstadosZona.put("Hidalgo", "ZonaSur");
        mapaEstadosZona.put("Tabasco", "ZonaSur");
        mapaEstadosZona.put("Chiapas", "ZonaSur");
    }

    public static String obtenerZonaPorEstado(String estado) {
        return mapaEstadosZona.getOrDefault(estado, "ZonaDesconocida");
    }
}
