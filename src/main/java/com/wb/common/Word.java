package com.wb.common;

public class Word {
    // id
    private Long id;
    // 词
    private String word;
    // 词所在的文件名
    private String filePath;
    private Integer cnt;


    public Word() {
    }

    public Word(Long id, String word, String filePath, Integer cnt) {
        this.id = id;
        this.word = word;
        this.filePath = filePath;
        this.cnt = cnt;
    }

    public Word(String word, String filePath) {
        this.word = word;
        this.filePath = filePath;
    }

    public Word(String word, String filePath, Integer cnt) {
        this.word = word;
        this.filePath = filePath;
        this.cnt = cnt;
    }

    public Word(Long id, String word, String filePath) {
        this.id = id;
        this.word = word;
        this.filePath = filePath;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Integer getCnt() {
        return cnt;
    }

    public void setCnt(Integer cnt) {
        this.cnt = cnt;
    }
}