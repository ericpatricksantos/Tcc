package main

import (
 "fmt"
 "math"
)

func main() {
fmt.Println(0 - 10)

// ultimoValor := (1512 / 1000.0)
//x, o := math.Modf(ultimoValor)
//fmt.Println(x)
//fmt.Println(o)
//
// fmt.Println("Funcao: ", UltimoValor(1511, 1000.0))
//
//fmt.Println("Funcao: ", ParteFracionario(1511, 1000.0))
}

func ParteFracionario(valorReal int, dividendo float32) int{
 valorRealFracionario := float32(valorReal) / dividendo
 valorRealInteiro := valorReal / int(dividendo)
 resultadoParcial := valorRealFracionario - float32(valorRealInteiro)
 resultado := int(resultadoParcial * dividendo)
 return resultado
}

func UltimoValor(valorReal int, dividendo float64) int{
 valor := (float64(valorReal) / dividendo)
 _,valorRealFracionario := math.Modf(float64(valor))
 resultado := int(valorRealFracionario * dividendo)
 return resultado
}